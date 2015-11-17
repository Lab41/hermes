#!/usr/bin/env python

"""Provides a Repository class to handle the downloading and removal of a
github repository.

This class should be used as follows:

    with Repository("lab41/hermes") as hermes_repo:
        # do_stuff()

The repository is downloaded to a temporary directory when the with block
enters, and is removed when the with block exits even if there is an error.

"""

import tempfile
import shutil
import subprocess


class Repository(object):
    """Manage the download and cleanup of a github.com git repository.

    Given the name of a github repository, this class will download it to a
    local, temporary directory. The directory will be cleaned up at the end.

    The correct way to use this class is with a with statement as follows:

        with Repository("lab41/hermes") as hermes_repo:
            pass

    This insures that the temporary directory is cleaned up regardless of
    exceptions.

    Attributes:
        name (str): The name of the github repository, for example
            "lab41/hermes"
        url (str): The url of the git repository.
        tempdir (str): The directory which houses the repository.

    """
    def __init__(self, name):
        """Initialize the class given a github repository name.

        Args:
            name (str): The name of the github repository, for example
                "lab41/hermes"
        """
        self.name = name
        self.tempdir = tempfile.mkdtemp()
        self.url = "https://github.com/" + self.name + ".git"

        self.__clone_remote()

    # enter and exit are called when the class is used in a "with" clause,
    # like:
    #
    # with Repository("lab41/hermes") as hermes_repo:
    #     pass
    #
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Clean up the directory
        shutil.rmtree(self.tempdir)

    def __clone_remote(self):
        command = [
            "git",
            "clone",
            "--",
            self.url,
            self.tempdir,
        ]
        subprocess.check_call(command)
