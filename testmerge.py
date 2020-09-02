#!/usr/bin/python3
"""
test the little merge toy
"""
import gzip
import os
import shutil
import unittest

from merge import MergeAdd


class MergeTestCase(unittest.TestCase):
    def setUp(self):
        '''create files and dirs used for testing'''
        os.makedirs('test/files')
        os.makedirs('test/output')
        # contents lists must be sorted by field 2 and 3 concatenated
        contents_list = [[1, 2, "This_is_not_an_article"],
                         [20, 4, "Neither is this"],
                         [3, 4, "This_isn't_either"],
                         [1, 8, "Joe_Schmuck"],
                         [1, 9, "Talk_to_Joe_Schmuck"]]
        self.write_contents(contents_list, 'test/files/have.txt')
        contents_list = [[1, 3, "This_is_an_interesting_article"],
                         [20, 4, "Neither is this"],
                         [3, 4, "This_isn't_either"],
                         [1, 9, "More_Joe_Schmuck"],
                         [1, 9, "Don't_Talk_to_Joe_Schmuck"],
                         [1, 9, "Talk_to_Joe_Schmuck"],
                         [1, 20, 'WHatever']]
        self.write_contents(contents_list, 'test/files/to_merge.txt')

    def write_contents(self, contents_list, path):
        '''given a list of contents, turn it into a bunch of tab separated lines
        and write to the specified path'''
        contents_with_tabs = ["\t".join(entry) for entry in contents_list]
        contents = "\n".join(contents_with_tabs) + "\n"
        with open(path, "w") as outfile:
            outfile.fwrite(contents)

    def tearDown(self):
        '''clean up all input and output files used for testing'''
        shutil.rmtree('test/files')
        shutil.rmtree('test/output')
        pass

    def test_merge(self):
        '''test the merge against known files'''
        keys = [2, 3]
        sums = [1]
        merger = MergeAdd(keys, sums)

        have = merger.get_file_handle('test/files/have.txt')
        to_merge = merger.get_file_handle('test/files/to_merge.txt')
        output = gzip.open('test/output/output.gz', 'wt')

        merger.do_merge(have, to_merge, output, keys, sums)
        

if __name__ == '__main__':
    unittest.main()
    
