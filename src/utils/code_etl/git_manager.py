#!/usr/bin/env python

"""Provides a Repository class to handle the cloning and removal of a git
repository.

This class should be used as follows:

    with Repository("git@github.com:Lab41/hermes.git") as hermes_repo:
        do_stuff()

The repository is cloned to a temporary directory when the with block enters,
and is removed when the with block exits even if there is an error.

"""

from cd import cd
import os
import shutil
import subprocess
import tempfile

#TODO: Replace with GitPython when we have a packaging solution.

class Repository(object):
    """Manage the cloning and cleanup of a git repository.

    Given the location of a git repository, this class will clone it to a
    local, temporary directory. The directory will be cleaned up at the end.

    The correct way to use this class is with a with statement as follows:

        with Repository("git@github.com:Lab41/hermes.git") as hermes_repo:
            do_stuff()

    This insures that the temporary directory is cleaned up regardless of
    exceptions.

    Attributes:
        remote_location (str): The location of the remote git repository.
        local_location (str): The location of the local repository.

    """
    def __init__(self, location):
        """Initialize the class given a git location.

        Args:
            location (str): The location of the repository to clone
        """
        self.__tempdir = tempfile.mkdtemp()
        self.remote_location = location
        self.local_location = None

        self.__clone_remote()

    # enter and exit are called when the class is used in a "with" clause,
    # like:
    #
    # with Repository("git@github.com:Lab41/hermes.git") as hermes_repo:
    #     pass
    #
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Clean up the directory
        shutil.rmtree(self.__tempdir)

    def __clone_remote(self):
        with cd(self.__tempdir):
            command = [
                "git",
                "clone",
                "--",
                self.remote_location,
            ]
            subprocess.check_call(command)

        # Set the local directory. The only item in the directory will be the
        # repository.
        items = os.listdir(self.__tempdir)
        self.local_location = self.__tempdir + '/' + items[0]
