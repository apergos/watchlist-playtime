#!/usr/bin/python3

"""
dump the unique namespace, title fields from the
watchlist table for a given wiki in batches,
merge the output files together, and produce
a single tab-separated output file with the number
 of times each article appears in the watchlist

dependencies: mlr (miller)
"""

import sys
import os
import getopt
import gzip
import shutil
import time
from subprocess import Popen, PIPE
from dumps.exceptions import BackupError
from dumps.wikidump import Config
from dumps.wikidump import Wiki
from dumps.utils import DbServerInfo


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """Usage: dump_watchlist.py --wiki <wikiname> --configfile <path>
          --output <path> [--batchsize <num>] [--tempdir <path>] [--verbose] [--dryrun]
          [--help]

--wiki       (-w):  name of db of wiki
--configfile (-c):  path to config file
--output     (-o):  path to final output file
--tempdir    (-t):  path to temporary directory where
                    intermediate files will be written
                    default: /tmp
--batchsize  (-b):  nuber of rows in each query
                    default: 1000
--verbose    (-v):  display messages about what the script is doing
--dryrun     (-d):  don't produce output, just display the commands that would be run
--help       (-h):  display this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_args():
    """
    get and validate args, and return them
    """
    wiki = None
    configfile = None
    output = None
    tempdir = '/tmp'
    batchsize = 1000
    verbose = False
    dryrun = False
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:w:o:t:b:vdh", ["configfile=", "wiki=", "output=", "batchsize=",
                                            "tempdir=", "verbose", "dryrun", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configfile = val
        elif opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-o", "--output"]:
            output = val
        elif opt in ["-o", "--tempdir"]:
            tempdir = val
        elif opt in ["-b", "--batchsize"]:
            batchsize = val
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-v", "--dryrun"]:
            dryrun = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    if remainder:
        usage("Unknown option specified")

    validate_args(wiki, output, configfile, batchsize, tempdir)

    return wiki, output, configfile, tempdir, batchsize, verbose, dryrun


def validate_args(wiki, output, configfile, batchsize, tempdir):
    '''
    check values of the args, whine and exit as needed
    '''
    if not wiki or not output or not configfile:
        usage("Arguments 'wiki', 'output' and 'configfile' must be set")

    if not batchsize.isdigit():
        usage("Batchsize argument must be an integer")

    if not os.path.exists(os.path.dirname(output)):
        usage("directory " + os.path.dirname(output) + " for output file does not exist")
    if not os.path.exists(tempdir):
        usage("path specified for 'tempdir' does not exist")


# fixme the default executable is hacky
def run_without_output(command, maxtries=3, retry_delay=5, shell="/bin/bash", verbose=False):
    """Run a shell command, expecting no output. Any output should be redirected
       to a file. Error output will be grabbed and written to stderr.
       Raises BackupError on non-zero return code."""

    if type(command).__name__ == 'list':
        command_string = " ".join(command)
    else:
        command_string = command
    if verbose:
        print("command to be run with no output: ", command_string)
    success = False
    error = "unknown"
    tries = 0
    while (not success) and tries < maxtries:
        proc = Popen(command, shell=True, stderr=PIPE, executable=shell)
        returncode = None
        while returncode is None:
            output, error = proc.communicate()
            if error is not None:
                error = error.decode('utf-8')
                if error:
                    print("error received from command", command_string, "is:", error)
            if output is not None:
                output = output.decode('utf-8')
            # FIXME we need to catch errors n stuff
            returncode = proc.returncode
        if not returncode:
            success = True
        else:
            time.sleep(retry_delay)
        tries = tries + 1
    if not success:
        raise BackupError("command '" + command_string +
                          ("' failed with return code %s " %
                           returncode) + " and error '" + error + "'")
    return success


class MergeAdder():
    '''merge two sorted watchlist files together, summing up the counts
    for articles in multiple watchlists'''
    def __init__(self, outdir, wiki):
        '''
        args:
          outdir - directory where output file will be written
          wiki   - Wiki object
        '''
        self.outdir = outdir
        self.wiki = wiki

    @staticmethod
    def create_empty_file(path):
        '''create an empty gzipped file to be the first file we merge into'''
        empty = gzip.open(path, "wt")
        empty.close()

    def merge(self, merge_into, to_merge, merged_path, verbose, dryrun):
        '''
        actually do the merge of two files

        args:
          merge_into  - full path of file which we will merge into
          to_merge    - full path of file to merge into the other one
          merged_path - where the merged output will be written
        '''
        command = "{sort} -t$'\t' -k1n -k2  -m ".format(sort=self.wiki.config.sort)
        command += '<({gzip} -dc {merge_into}) <({gzip} -dc {to_merge}) '.format(
            gzip=self.wiki.config.gzip, merge_into=merge_into, to_merge=to_merge)

        mlr_base = "{mlr} --tsvlite -N ".format(mlr=self.wiki.config.mlr)
        command += " |  {mlr_base} stats1 -g 1,2 -f 3 -a sum ".format(mlr_base=mlr_base)

        command += " | {gzip} > {output}".format(gzip=self.wiki.config.gzip, output=merged_path)
        if dryrun:
            print("would run command:", command)
        else:
            run_without_output(command, maxtries=3, verbose=verbose)


class QueryRunner():
    '''
    run a query, output goes to a file
    '''
    def __init__(self, outdir, wiki, dbserver):
        '''
        args:
          outdir    - directory where output file will be written
          wiki      - Wiki object
          dbserver  - DbServerInfo object
        '''
        self.outdir = outdir
        self.wiki = wiki
        self.dbserver = dbserver

    def do_one_query(self, query, outpath, verbose, dryrun):
        '''
        set up and run one dump query

        args:
          query   - sql query as text string
          outpath - full path to file to contain output from query
          verbose - write progress messages
          dryrun  - display the commands that would be run
        '''
        pipeto = self.wiki.config.gzip + " > " + outpath
        command = self.dbserver.build_sql_command(query, pipeto)
        if not isinstance(command, str):
            # see if the list elts are lists that need to be turned into strings
            command = [element if isinstance(element, str)
                       else ' '.join(element) for element in command]
            command = ' | '.join(command)
            # FIXME horrible! should add mysql_extra_parameters() method to dumps/utils.py
            # and add an additional option to build_sql_command in the same file
            command = command.replace('/bin/mysql', '/bin/mysql -N', 1)
        if dryrun:
            print("Would run:", command)
            return
        if verbose:
            print("Running", command)

        run_without_output(command, maxtries=3, verbose=verbose)

    def get_max_id(self, fieldname, tablename):
        '''
        get and return MAX of the field in the specified table or None if
        there are no rows or other error
        '''
        query = "select MAX({field}) from {table};".format(field=fieldname, table=tablename)

        results = self.dbserver.run_sql_and_get_output(query)
        if results:
            lines = results.splitlines()
            if lines and lines[1]:
                if not lines[1].isdigit():
                    return None   # probably NULL or missing table
                return int(lines[1])
        return None


class WatchlistDumper():
    '''
    methods for dumping the watchlist table
    '''
    def __init__(self, outdir, batchsize, wiki, querier, merger):
        '''
        args:
          outdir    - directory where output file will be written
          batchsize - how many records to dump at once
          wiki      - Wiki object
          querier   - QueryRunner object
          merger    - MergeAdder object
        '''
        self.outdir = outdir
        self.wiki = wiki
        self.querier = querier
        self.batchsize = batchsize
        self.merger = merger

    def dump_merge_batch(self, count, startrow, maxid,
                         merge_into_path, verbose, dryrun):
        '''
        do a number of dump and merge operations, up to 'count'
        or fewer if we run out of items to dump

        return the next starting row

        if we produce no output file because we are already out of
        items to dump, return None

        args:
          count           - number of files to do in the batch
          startrow        - first row of the watchlist table to start dump
                            for this batch
          maxid           - max watchlist id, we stop dumping after this
          merge_into_path - path to file that will be merged into
          verbose         - write some progress messages
          dryrun          - display commands that would be run
        '''
        index = 1
        # we start with the file we are merging into being empty, as we have
        # no data generated yet.

        if not dryrun:
            self.merger.create_empty_file(merge_into_path)

        dumped_path = os.path.join(self.outdir, '{wiki}-watchlist-dumped.gz'.format(
            wiki=self.wiki.db_name))

        if startrow >= maxid or index >= count:
            return None

        while startrow < maxid and index < count:
            end = startrow + int(self.batchsize)
            query = ("'select wl_namespace, wl_title, COUNT(*)  FROM watchlist WHERE "
                     "wl_id >= {start} AND wl_id < {end} GROUP BY wl_namespace, wl_title "
                     "ORDER BY wl_namespace, wl_title ASC'".format(
                         start=startrow, end=end))
            self.querier.do_one_query(query, dumped_path, verbose, dryrun)
            merged_path = os.path.join(self.outdir, "{wiki}-watchlist-merged.gz".format(
                wiki=self.wiki.db_name))
            self.merger.merge(merge_into_path, dumped_path, merged_path,
                              verbose, dryrun)
            if not dryrun:
                shutil.move(merged_path, merge_into_path)

            startrow = end
            index += 1

        if not dryrun:
            os.unlink(dumped_path)

        return startrow

    def dump_watchlist(self, file_batch_count, out_path, verbose, dryrun):
        '''
        dump watchlist table for a wiki in chunks to a specified output directory

        we merge at two levels. we dump and merge file_batch_count number of files
        pairwise, and then as we accumulate the results of that we merge those files
        pairwise.
        '''
        maxid = self.querier.get_max_id('wl_id', 'watchlist')
        if not maxid:
            print("Failed to get max watchlist id, bailing!")
            sys.exit(1)

        if verbose or dryrun:
            print("Max watchlist id:", maxid)

        # these are for the final merge of files.
        all_merge_into_path = os.path.join(self.outdir, "{wiki}-mergeinto-all.gz".format(
            wiki=self.wiki.db_name))

        if not dryrun:
            self.merger.create_empty_file(all_merge_into_path)

        batch_merged_path = os.path.join(self.outdir, "{wiki}-watchlist-merged.gz".format(
            wiki=self.wiki.db_name))

        # this is for the merge of files in each batch
        batch_merge_into_path = os.path.join(
            self.outdir, "{wiki}-batch-mergeinto.gz".format(wiki=self.wiki.db_name))

        startrow = 1
        while startrow and startrow < maxid:
            # here we dump and merge
            if verbose:
                print("starting dump-merge batch with row", startrow)
            startrow = self.dump_merge_batch(file_batch_count, startrow, maxid,
                                             batch_merge_into_path, verbose, dryrun)
            if startrow:
                # the results of the batch get merged into our final file
                if verbose:
                    print("merging results of dump-merge batch")
                self.merger.merge(all_merge_into_path, batch_merge_into_path, batch_merged_path,
                                  verbose, dryrun)
                if not dryrun:
                    shutil.move(batch_merged_path, all_merge_into_path)

        if dryrun:
            print("output would be moved from", all_merge_into_path, "to", out_path)
            return

        shutil.move(all_merge_into_path, out_path)
        if verbose:
            print("output available in", out_path)


class TitleFilter():
    '''
    filter a sorted file of watchlist entries by a file of good ns title pairs
    '''
    def __init__(self, outdir, wiki, querier):
        '''
        args:
          outdir  - directory where output file will be written
          wiki    - Wiki object
          querier - QueryRunner object
        '''
        self.outdir = outdir
        self.wiki = wiki
        self.querier = querier

    def dump_titles(self, out_path, verbose, dryrun):
        '''
        we actually have this as a job in the two monthly runs already
        but let's add it in here anyways for toy testing purposes
        '''
        query = ("'SELECT page_namespace, page_title FROM page ORDER BY "
                 "page_namespace, page_title ASC'")
        self.querier.do_one_query(query, out_path, verbose, dryrun)

    def do_filter(self, watchlist_dump_path, filtered_path, verbose, dryrun):
        '''
        given a sorted tsv file of ns, title, count, dump all the titles, then
        generate an output file of rows in the first file where ns, title
        are in the second file and skip all other rows
        '''
        titles_path = os.path.join(self.outdir, "{wiki}-titles.gz".format(wiki=self.wiki.db_name))
        self.dump_titles(titles_path, verbose, dryrun)

        # decompress watchlist. sadness! but mlr won't dtrt
        watchlist_uncompressed_path = watchlist_dump_path[:-3]
        command = "{gzip} -dc {infile} > {outfile}".format(gzip=self.wiki.config.gzip,
                                                           infile=watchlist_dump_path,
                                                           outfile=watchlist_uncompressed_path)
        if dryrun:
            print("would run", command)
        else:
            run_without_output(command, maxtries=3, verbose=verbose)

        mlr_base = "{mlr} --tsvlite -N --prepipe '{gzip} -dc'".format(
            mlr=self.wiki.config.mlr, gzip=self.wiki.config.gzip)
        command = "{mlr_base} join -j 1,2 -s -f {watchlist} {titles}".format(
            mlr_base=mlr_base, watchlist=watchlist_uncompressed_path, titles=titles_path)
        command += " |  {gzip} > {output}".format(gzip=self.wiki.config.gzip, output=filtered_path)

        if dryrun:
            print("would run", command)
        else:
            run_without_output(command, maxtries=3, verbose=verbose)
            # get rid of work files
            os.unlink(titles_path)
            os.unlink(watchlist_uncompressed_path)


def do_main():
    """entry point:
    get args, run query repeatedly, exit
    """
    wiki, output, configfile, tempdir, batchsize, verbose, dryrun = get_args()

    wiki = Wiki(Config(configfile), wiki)
    # fixme actually add these to the config if we go this route
    wiki.config.sort = '/usr/bin/sort'
    wiki.config.mlr = '/home/ariel/mlr'
    dbserver = DbServerInfo(wiki, wiki.db_name)
    merger = MergeAdder(tempdir, wiki)
    querier = QueryRunner(tempdir, wiki, dbserver)
    dumper = WatchlistDumper(tempdir, batchsize, wiki, querier, merger)

    dumped_merged_path = os.path.join(
        tempdir, "{wiki}-watchlist-dumped-merged.gz".format(wiki=wiki.db_name))
    if verbose:
        print("beginning dump of watchlist and file merges")
    dumper.dump_watchlist(100, dumped_merged_path, verbose, dryrun)

    titlefilter = TitleFilter(tempdir, wiki, querier)
    if verbose:
        print("beginning filtering of watchlist entries against existing titles")
    titlefilter.do_filter(dumped_merged_path, output, verbose, dryrun)
    if not dryrun:
        os.unlink(dumped_merged_path)

    # fixme there is a dump file left around which should get cleaned up


if __name__ == '__main__':
    do_main()
