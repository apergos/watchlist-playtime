#!/usr/bin/python3

"""
quick and dirty dump the unique namespace, title fields from the
watchlist table for a given wiki
"""

import sys
import os
import getopt
from dumps.wikidump import Config
from dumps.wikidump import Wiki
from dumps.utils import DbServerInfo, RunSimpleCommand


def usage(message=None):
    '''
    display a helpful usage message with
    an optional introductory message first
    '''
    if message is not None:
        sys.stderr.write(message)
        sys.stderr.write("\n")
    usage_message = """
Usage: dump_watchlist_only.py --wiki <wikiname>
        --configfile <path> [--verbose] [--help]

--wiki       (-w):  name of db of wiki
--configfile (-c):  path to config file
--outdir     (-o):  path to output directory for files
--batchsize  (-b):  nuber of rows in each query
--verbose    (-v):  display messages about what the script is doing
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
    outdir = None
    batchsize = None
    verbose = False
    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "c:w:o:b:vh", ["configfile=", "wiki=", "outdir=", "batchsize=",
                                         "verbose", "help"])
    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-c", "--configfile"]:
            configfile = val
        elif opt in ["-w", "--wiki"]:
            wiki = val
        elif opt in ["-o", "--outdir"]:
            outdir = val
        elif opt in ["-b", "--batchsize"]:
            batchsize = val
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage("Help for this script")

    if remainder:
        usage("Unknown option specified")

    if not wiki or not outdir or not configfile:
        usage("Arguments 'wiki', 'outdir' and 'configfile' must be set")

    if not batchsize.isdigit():
        usage("Batchsize argument must be an integer")
    batchsize = int(batchsize)

    if not os.path.exists(outdir):
        usage("path specified for 'outdir' does not exist")

    return(wiki, outdir, configfile, batchsize, verbose)


class WatchlistDumper():
    '''
    methods for dumping the watchlist table
    '''
    def __init__(self, outdir, wiki, configfile, batchsize):
        self.outdir = outdir
        self.batchsize = batchsize
        self.wiki = Wiki(Config(configfile), wiki)
        self.dbserver = DbServerInfo(self.wiki, self.wiki.db_name)

    def get_max_watchlist_id(self):
        '''
        get and return MAX wl_id or None if there are no rows
        or other error
        '''
        query = "select MAX(wl_id) from watchlist;"

        results = self.dbserver.run_sql_and_get_output(query)
        if results:
            lines = results.splitlines()
            if lines and lines[1]:
                if not lines[1].isdigit():
                    return None   # probably NULL or missing table
                return int(lines[1])
        return None

    def dump(self, verbose):
        '''
        dump watchlist table for a wiki in chunks to a specified output directory
        '''
        maxid = self.get_max_watchlist_id()
        if not maxid:
            print("Failed to get max watchlist id, bailing!")
            sys.exit(1)

        if verbose:
            print("Max watchlist id:", maxid)

        start = 1
        while start < maxid:
            end = start + self.batchsize
            query = ("'select wl_namespace, wl_title, COUNT(*)  FROM watchlist WHERE "
                     "wl_id >= {start} AND wl_id < {end} GROUP BY wl_namespace, wl_title "
                     "ORDER BY wl_namespace, wl_title ASC'".format(
                         start=start, end=end))
            outfile_name = "{wiki}-watchlist-{start}-{end}.gz".format(
                wiki=self.wiki.db_name, start=str(start), end=str(end))
            outfile_path = os.path.join(self.outdir, outfile_name)
            pipeto = self.wiki.config.gzip + " > " + outfile_path
            command = self.dbserver.build_sql_command(query, pipeto)
            if not isinstance(command, str):
                # see if the list elts are lists that need to be turned into strings
                command = [element if isinstance(element, str)
                           else ' '.join(element) for element in command]
                command = '|'.join(command)
                # horrible! but this is just a hack anyways
                command = command.replace('/bin/mysql', '/bin/mysql -N', 1)
            if verbose:
                print("Running", command)

            RunSimpleCommand.run_with_no_output(command, maxtries=1, shell=True, verbose=verbose)
            start = end


def do_main():
    """entry point:
    get args, run query repeatedly, exit
    """
    wiki, outdir, configfile, batchsize, verbose = get_args()

    dumper = WatchlistDumper(outdir, wiki, configfile, batchsize)
    dumper.dump(verbose)


if __name__ == '__main__':
    do_main()
