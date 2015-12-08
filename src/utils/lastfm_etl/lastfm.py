#!/usr/bin/env python

"""Translate the Last.fm data files to JSON.

This script takes the various Last.fm data files and write them out as
JSON. It removes the Last.fm artist URLs.

Attributes:
    ARTISTS (dict): A dictionary that stores information about the artists. The
        variables are as follows:
            - artist_id (int): A unique identifier for each artist.
            - name (str): The name of the artist.

    FRIENDS (dict): A dictionary that stores information about the friends
        graph. The variables are as follows:
            - user_id (int): A unique identifier for each user.
            - friend_user_id (int): A unique identifier of a user on the
                friends list.

    TAGS (dict): A dictionary that stores information about the tags. The
        variables are as follows:
            - tag_id (int): A unique identifier for each tag.
            - name (int): The name of the tag.

    PLAYS (dict): A dictionary that stores information about the number of
        plays by each user. The variables are as follows:
            - user_id (int): A unique identifier for each user.
            - artist_id (int): A unique identifier for each artist.
            - plays (int): The number of plays by the user of the artist.

    APPLIED_TAGS (dict): A dictionary that stores information about the tags
        various users applied to various artists. The variables are as follows:
            - user_id (int): A unique identifier for each user.
            - artist_id (int): A unique identifier for each artist.
            - tag_id (int): A unique identifier for each tag.
            - day (int): The day the tag was added.
            - month (int): The month the tag was added.
            - year (int): The year the tag was added.

"""

from copy import deepcopy
import json
import csv


# JSON objects
ARTISTS = {
    "artist_id": None,
    "name": None,
}

FRIENDS = {
    "user_id": None,
    "friend_user_id": None,
}

TAGS = {
    "tag_id": None,
    "name": None,
}

PLAYS = {
    "user_id": None,
    "artist_id": None,
    "plays": None,
}

APPLIED_TAGS = {
    "user_id": None,
    "artist_id": None,
    "tag_id": None,
    "day": None,
    "month": None,
    "year": None,
}


def convert_str(string):
    """Convert a string from 'iso-8859-1' to 'utf8'."""
    return string.decode('iso-8859-1').encode('utf8')


def iter_lines(open_file):
    """Open the Last.fm CSVs and return an iterator over the lines.

    Args:
        open_file: A file handle object from open().

    Retunrs:
        iterator: An iterator over each line in the file. Each line is a list,
            with string elements for each column value.

    """
    reader = csv.reader(
        open_file,
        delimiter='\t',
    )
    next(reader)  # Skip the header

    return reader


def parse_artist_line(line):
    """Parse a line from the Artist CSV file.

    A line is a list of strings as follows:

        line = [
            artist_id,
            name,
            band_url,
            band_photo_url,
        ]

    Args:
        lines (list): A list of strings as described above.

    Returns:
        dict: A dictionary containing the keys "artist_id" and
            "name".

    """
    (artist_id, name, _, _) = line
    current_artist = deepcopy(ARTISTS)
    current_artist["artist_id"] = int(artist_id)
    current_artist["name"] = name

    return current_artist


def parse_friends_line(line):
    """Parse a line from the Friends CSV file.

    A line is a list of strings as follows:

        line = [
            user_id,
            user_id_of_friend,
        ]

    Args:
        lines (list): A list of strings as described above.

    Returns:
        dict: A dictionary containing the keys "user_id" and "friend_user_id".

    """
    (user_id, friend_id) = line
    current_friend = deepcopy(FRIENDS)
    current_friend["user_id"] = int(user_id)
    current_friend["friend_user_id"] = int(friend_id)

    return current_friend


def parse_tag_line(line):
    """Parse a line from the Tag CSV file.

    A line is a list of strings as follows:

        line = [
            tag_id,
            tag,
        ]

    Args:
        lines (list): A list of strings as described above.

    Returns:
        dict: A dictionary containing the keys "tag_id" and "tag".

    """
    (tag_id, tag) = line
    current_tag = deepcopy(TAGS)
    current_tag["tag_id"] = int(tag_id)
    current_tag["name"] = convert_str(tag)

    return current_tag


def parse_applied_tag_line(line):
    """Parse a line from the Applied Tags CSV file.

    A line is a list of strings as follows:

        line = [
            user_id,
            artist_id,
            tag_id,
            day,
            month,
            year,
        ]

    Args:
        lines (list): A list of strings as described above.

    Returns:
        dict: A dictionary containing the keys "user_id", "artist_id",
            "tag_id", "day", "month", and "year".

    """
    (user_id, artist_id, tag_id, day, month, year) = line
    current_tag = deepcopy(APPLIED_TAGS)
    current_tag["user_id"] = int(user_id)
    current_tag["artist_id"] = int(artist_id)
    current_tag["tag_id"] = int(tag_id)
    current_tag["day"] = int(day)
    current_tag["month"] = int(month)
    current_tag["year"] = int(year)

    return current_tag


def parse_plays_line(line):
    """Parse a line from the Played Artists CSV file.

    A line is a list of strings as follows:

        line = [
            user_id,
            artist_id,
            play_count,
        ]

    Args:
        lines (list): A list of strings as described above.

    Returns:
        dict: A dictionary containing the keys "user_id", "artist_id", and
            "plays".

    """
    (user_id, artist_id, plays) = line
    current_plays = deepcopy(PLAYS)
    current_plays["user_id"] = int(user_id)
    current_plays["artist_id"] = int(artist_id)
    current_plays["plays"] = int(plays)

    return current_plays


if __name__ == "__main__":

    import argparse

    # Set up command line flag handling
    parser = argparse.ArgumentParser(
            description="Transform the Last.FM datasets to JSON",
        )
    parser.add_argument(
            'artists',
            type=str,
            help="the file containing the artists, normally 'artists.dat'",
            )
    parser.add_argument(
            'tags',
            type=str,
            help="the file containing the tags, normally 'tags.dat'",
            )
    parser.add_argument(
            'friends',
            type=str,
            help="the file containing the friends graph, normally 'user_friends.dat'",
            )
    parser.add_argument(
            'applied_tags',
            type=str,
            help="the file containing the applied tags, normally 'user_taggedartists.dat'",
            )
    parser.add_argument(
            'plays',
            type=str,
            help="the file containing the play counts, normally 'user_artists.dat'",
            )
    parser.add_argument(
            '-o',
            '--output_directory',
            type=str,
            action="store",
            help="the directory to save the output JSON files, by default the current directory",
            default="./",
            )

    args = parser.parse_args()

    # Parse the files
    processing_queue = (
        (args.artists, args.output_directory + "/artists.json", parse_artist_line),
        (args.tags, args.output_directory + "/tags.json", parse_tag_line),
        (args.friends, args.output_directory + "/friends.json", parse_friends_line),
        (args.applied_tags, args.output_directory + "/applied_tags.json", parse_applied_tag_line),
        (args.plays, args.output_directory + "/plays.json", parse_plays_line),
    )
    for input_file, output_file, function in processing_queue:
        with open(input_file, 'rb') as csv_file, open(output_file, 'w') as json_file:
            for row in iter_lines(csv_file):
                json_file.write(json.dumps(function(row)) + '\n')
