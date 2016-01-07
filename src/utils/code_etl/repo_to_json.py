#!/usr/bin/env python

"""Translate a git repository of Python code to JSON.

This script takes a git repository--either a local repository or a remote one
on github--and converts each line into a JSON object containing information
about the line, the author, and the committer.

Note that a github repository is saved to a local temporary directory for
processing and is removed when the processing is complete. Local repositories
are never deleted.

The JSON object for each line has the following form:

    {
        "repo_name": val,
        "commit_id": val,
        "author": val,
        "author_mail": val,
        "author_time": val,
        "author_timezone": val,
        "committer": val,
        "committer_mail": val,
        "committer_time": val,
        "committer_timezone": val,
        "comment": val,
        "filename": val,
        "line_num": val,
        "line": val,
    }

    Where the variables are as follows:
        repo_name (str): The name of the repository.
        commit_id (str): The commit hash.
        author (str): The author of the commit.
        author_mail (str): The author's email address.
        author_time (int): The time the commit was written in seconds
            since the unix epoch.
        author_timezone (int): The timezone of the author, as an integer.
            The two right most digits are the minute offsets, and the
            remaining leftmost digits are the hour offsets.
        committer (str): The committer of the commit.
        committer_mail (str): The committer's email address.
        committer_time (int): The time the commit was written in seconds
            since the unix epoch.
        committer_timezone (int): The timezone of the committer, in the
            same form as author_timezone.
        comment (str): The commit message.
        filename (str): The name of the file.
        line_num (int): The line number of the text in the file after the
            commit was applied.
        line (str): The text of the source line.

Examples:

    With a local repository:

        ./repo_to_json.py /path/to/local/repo/ /output/directory/

    For a github repository:

        ./repo_to_json.py username/reponame /output/directory/

    The repo_name field in the JSON object is either the basename of the local
    path to the repository ("repo" in the case of the first example) or the
    name of the github repository ("username/reponame" in the case of the
    second example). This can be overridden to use a custom name with the
    commandline flag `--repo-name` (also `-n`). This is done as follows:

        ./repo_to_json.py username/reponame /output/directory/ -n "New Name"

"""

from cd import cd
import blame_to_json as btj
import git_manager as gm
import user_to_file_mapper as ufm
import os
import os.path
import subprocess
import sys
import errno


def get_filelist(directory):
    """Return a list of python files in a directory structure.

    Args:
        directory (str): The directory to start the search in.

    Returns:
        list of strings: A list of the locations of all the python files in the
            directory tree.
    """
    file_list = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_list.append(root + '/' + file)

    return file_list


def is_path_exist(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def process_local_repo(location, output_dir, repo_name):
    """Convert a local repository to a series of JSON objects.

    Args:
        location (str): The path to a local repository.
        output_dir (str): The path to the directory to save the output files
            to.
        repo_name (str): The name to save to the JSON objects as the repository
            name.

    Returns:
        None

    """
    with cd(location):
        is_path_exist(output_dir)

        # Produce a JSON object from the blame of each file
        output_file = output_dir + "/" + repo_name.replace('/', '_') + ".json"
        with open(output_file, 'w') as f:
            for file in get_filelist(location):
                for line in btj.file_to_json(file, location, repo_name):
                    f.write(line + "\n")

        # Produce a map of files to the users who edited it
        output_file_map = output_dir + "/" + repo_name.replace('/', '_') + "_file_to_user_map.json"
        with open(output_file_map, 'w') as f:
            for line in ufm.repo_to_file_map_json(repo_name):
                f.write(line + "\n")


def get_local_repo_name(location):
    """The basename of a local repository.

    If a local repository is located at:

        /path/to/local/repo/

    This function will return "repo".

    This function must be called from within the repository, so using cd() to
    change the directory is advised.

    Args:
        location (str): The path to a local repository.

    Returns:
        str: The basename of the repository.

    """
    with cd(location):
        command = [
            "git",
            "rev-parse",
            "--show-toplevel",
        ]
        repo_name = subprocess.check_output(command)
        base = os.path.basename(repo_name).strip()
        return base


if __name__ == "__main__":

    import argparse
    # Set up command line flag handling
    parser = argparse.ArgumentParser(
        description="Clone and parse a git repository",
    )
    parser.add_argument(
        'repo_location',
        type=str,
        help="the location of the repository"
    )
    parser.add_argument(
        'output_directory',
        type=str,
        action="store",
        help="the directory to save the output JSON files",
    )
    parser.add_argument(
        '-n',
        '--repo-name',
        type=str,
        action="store",
        help="override the default repository name to save to the JSON file"
    )

    args = parser.parse_args()

    # If the user has passed in a repo name, override the default name
    repo_name = args.repo_name

    # If we are given a valid local path, then use that
    with gm.Repository(args.repo_location) as repo:
        if not repo_name:
            repo_name = get_local_repo_name(repo.local_location)
        process_local_repo(
            repo.local_location,
            args.output_directory,
            repo_name
        )
