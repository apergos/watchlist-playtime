#!/usr/bin/python3
'''
toy implementation of a merger/accumulator which uses
the most naive approach possible
'''

import getopt
import os
import sys
import gzip


def usage(message=None):
    '''
    display a nice usage message along with an optional message
    describing an error
    '''
    if message:
        sys.stderr.write(message + "\n")
    usage_message = """Usage: $0 --indir <path> --intkeys <num[,num...]> --strkeys <num[,num...]>
        [--sums <num[,num...]>] [--verbose]

Reads the plain text or gzipped files in indir, in sorted order,
and produces a single merged list to stdout, possibly computing the sum of
values in certain fields of each record and writing the combined entries.

Entries are sorted by the tab-separated keys designated by --intkeys and --strkeys,
sorted by integer or string values respectively; field numbers start with 0. Tthe
input files must have already been sorted according to these keys.

If --sum is supplied, the values in these fields nust be numeric in every record and
will be totalled up for records identical in all other fields; the resultant list,
ordered by keys, will be written to stdout.

Arguments:

 --indir   (-i):  path to directory containing input files, possible gzipped
 --intkeys (-I):  comma-separated list of integer fields that were used to sort the input
                  files and should be used to order the output; fields start with 1
 --strkeys (-S):  comma-separated list of string fields that were used to sort the input
                  files and should be used to order the output; fields start with 1
 --sums    (-s):  comma-separated list of numeric fields which should be summed up
                  across otherwise identical records, and a list of the unique records
                  and their sums produced
--output   (-o):  path to gzipped output file
 --verbose (-v):  write some progress messages some day
 --help    (-h):  show this help message
"""
    sys.stderr.write(usage_message)
    sys.exit(1)


def get_args():
    '''get and validate command line args'''
    indir = None
    intkeys = None
    strkeys = None
    sums = None
    output = None
    verbose = False

    try:
        (options, remainder) = getopt.gnu_getopt(
            sys.argv[1:], "i:I:S:s:o:vh", ["indir=", "intkeys=", "strkeys=",
                                           "sums=", "output=", "verbose", "help"])

    except getopt.GetoptError as err:
        usage("Unknown option specified: " + str(err))

    for (opt, val) in options:
        if opt in ["-i", "--indir"]:
            indir = val
        elif opt in ["-I", "--intkeys"]:
            intkeys = val.split(',')
        elif opt in ["-S", "--strkeys"]:
            strkeys = val.split(',')
        elif opt in ["-s", "--sums"]:
            sums = val.split(',')
        elif opt in ["-o", "--output"]:
            output = val
        elif opt in ["-v", "--verbose"]:
            verbose = True
        elif opt in ["-h", "--help"]:
            usage('Help for this script\n')
        else:
            usage("Unknown option specified: <%s>" % opt)

    if remainder:
        usage("Unknown option(s) specified: {opt}".format(opt=remainder[0]))

    return indir, intkeys, strkeys, sums, output, verbose


def validate_args(indir, intkeys, strkeys, sums, output):
    '''validate args and whine about bad values'''
    if not indir:
        usage("No value specified for the mandatory arg indir")
    if not intkeys and not strkeys:
        usage("At least one of intkeys or strkeys must be specified")
    if not output:
        usage("No value specified for the mandatory arg output")
    for entry in intkeys:
        if not entry.isdigit():
            usage("Bad value for --intkeys, fields must be numeric")
    for entry in strkeys:
        if not entry.isdigit():
            usage("Bad value for --strkeys, fields must be numeric")
    for entry in sums:
        if not entry.isdigit():
            usage("Bad value for --sums, fields must be numeric")
    if not os.path.exists(indir):
        usage("Bad value for --indir, no such directory")


def get_filehandle(path):
    '''get a gz reader or a plain text filehandle for the
    specified file'''
    if path.endswith('.gz'):
        openwith = gzip.open
    else:
        openwith = open

    try:
        return openwith(path, "rt")
    except Exception:  # pylint: disable=broad-except
        return None


def whine(message):
    '''write a message to stderr and exit'''
    sys.stderr.write(message + '\n')
    sys.exit(1)


class MergeAdd():
    '''merge lines and accumulate sums from specified fields
    over two already sorted files'''
    def __init__(self, intkeys, strkeys, sums):
        self.intkeys = self.safe_assign(intkeys)
        self.strkeys = self.safe_assign(strkeys)
        self.sums = self.safe_assign(sums)

    @staticmethod
    def safe_assign(some_iter):
        '''convert a list of string format ints to a list of ints,
        handling properly the None case'''

        if some_iter is None:
            return []
        return [int(item) for item in some_iter]

    def compare_fields(self, one_fields, two_fields):
        '''
        compare two lists based on specific fields
        field numbering starts from 0

        returns:
          0 if equal
          -1 if first comes before second in the current locale
          1 if second comes before first
        '''
        for index in self.intkeys:
            if int(one_fields[index]) < int(two_fields[index]):
                return -1
            if int(one_fields[index]) > int(two_fields[index]):
                return 1
        for index in self.strkeys:
            if one_fields[index] < two_fields[index]:
                return -1
            if one_fields[index] > two_fields[index]:
                return 1

        if len(one_fields) > len(two_fields):
            return 1
        if len(one_fields) < len(two_fields):
            return -1
        return 0

    def sum_fields(self, infields, to_add_fields):
        '''
        replace fields in first list with sum of those fields with
        corresponding fields in second list, and return it

        both lists must have the same number of fields

        fields are numbered from 0
        '''
        if len(infields) != len(to_add_fields):
            whine("Lines in files with differing number of fields {one}, {two}\n".format(
                one=len(infields), two=len(to_add_fields)))

        for index in self.sums:
            try:
                infields[index] = str(int(infields[index]) + int(to_add_fields[index]))
            except ValueError:
                whine("Bad line in files, trying to add {one}, {two}\n".format(
                    one=infields[index], two=to_add_fields[index]))

        return infields

    def do_merge(self, have, to_merge, output):
        '''merge one file into the other, sorting by specified key
        fields and accumulating sums if specified'''
        have_fields = have.readline().rstrip('\n').split('\t')
        to_merge_fields = to_merge.readline().rstrip('\n').split('\t')
        while have_fields != [''] and to_merge_fields != ['']:
            result = self.compare_fields(have_fields, to_merge_fields)
            if not result:
                have_fields = self.sum_fields(have_fields, to_merge_fields)
                to_merge_fields = to_merge.readline().rstrip('\n').split('\t')
            elif result < 0:
                output.write('\t'.join(have_fields) + '\n')
                have_fields = have.readline().rstrip('\n').split('\t')
            elif result > 0:
                output.write('\t'.join(to_merge_fields) + '\n')
                to_merge_fields = to_merge.readline().rstrip('\n').split('\t')

        # we hit at least one eof
        have_line = '\t'.join(have_fields)
        to_merge_line = '\t'.join(to_merge_fields)
        while have_line:
            output.write(have_line + '\n')
            have_line = have.readline().rstrip('\n')
        while to_merge_line:
            output.write(to_merge_line + '\n')
            to_merge_line = to_merge.readline().rstrip('\n')


def do_main():
    '''entry point'''
    indir, intkeys, strkeys, sums, output, verbose = get_args()
    validate_args(indir, intkeys, strkeys, sums, output)
    infiles = os.listdir(indir)
    if not infiles:
        whine("Empty directory supplied, giving up")

    startfile = os.path.join(indir, infiles[0])
    tempfile = '/tmp/intermediate.gz'
    nextinput = '/tmp/nextinput.gz'

    if len(infiles) == 1:
        whine("Only one file provided for merging. Done already!")

    merger = MergeAdd(intkeys, strkeys, sums)

    for filename in infiles[1:]:
        if verbose:
            print("Processing files:", startfile, filename)
        have = get_filehandle(startfile)
        if not have:
            whine("Failed to open file {fname} for read, giving up".format(fname=have))
        to_merge = get_filehandle(os.path.join(indir, filename))
        if not to_merge:
            whine("Failed to open file {fname} for read, giving up".format(fname=filename))
        tempout = gzip.open(tempfile, 'wt')
        if not tempout:
            whine("Failed to open file {fname} for write, giving up".format(fname=tempfile))

        merger.do_merge(have, to_merge, tempout)
        have.close()
        to_merge.close()
        tempout.close()
        os.rename(tempfile, nextinput)
        startfile = nextinput

    os.rename(nextinput, output)
    if verbose:
        print("Final output in file", output)


if __name__ == '__main__':
    do_main()
