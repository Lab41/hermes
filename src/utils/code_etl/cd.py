#!/usr/bin/env python

"""Provides a cd class to change the working directory and return to the
starting directory.

This object should be used with the python "with" keyword as follows:

    with cd("/target/path/to/change/to"):
        # do stuff

"""

import os


class cd(object):
    """Object to change the current working directory safely.

    This object should be used with the python "with" keyword as follows:

        with cd("/target/path/to/change/to"):
            # do stuff

    The working directory will change to the target when entering the with
    block, and will change back to the current directory when it exits, even on
    error.
    """
    def __init__(self, directory):
        self.directory = directory

    def __enter__(self):
        self.cwd = os.getcwd()
        os.chdir(self.directory)

    def __exit__(self, exc_type, exc_value, traceback):
        os.chdir(self.cwd)
