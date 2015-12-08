#!/usr/bin/env python

"""Translate the Jester dataset to JSON.

This script takes various files from Jester (Dataset 1, Dataset 2, and Dataset
2+) and writes each user's rating as a single JSON object. Each joke is also
saved as a JSON object.

Attributes:
    RATINGS (dict): A dictionary that stores information from all of
        the rating actions taken by the users in the dataset. The
        variables are as follows:
            - user_id (int): A unique identifier for each user.
            - joke_id (int): A unique identifier for each joke.
            - rating (float): The user's rating for a joke,
                from -10 to 10.
     JOKES (dict): A dictionary that stores information about all the jokes in
        the dataset. The variables are as follows:
            - joke_id (int): A unique identifier for each joke.
            - joke_text (str): The text of the joke.

"""

from copy import deepcopy
import json
import xlrd
import bs4


# JSON rating object
RATINGS = {
    "user_id": None,
    "joke_id": None,
    "rating": None,
}

# JSON movie object
JOKES = {
    "joke_id": None,
    "joke_text": None,
}


def iter_rows(*args):
    """Return an iterator over the rows of the first sheet of a set of XLS
    files.

    Args:
        *args (str): The paths to a set of XLS file.

    Yields:
        list: A list of the contents of a row.
    """
    for xls_path in args:
        workbook = xlrd.open_workbook(xls_path)
        sheet = workbook.sheet_by_index(0)
        for i in range(sheet.nrows):
            yield sheet.row_values(i)
        del sheet, workbook


def row_to_jsons(row, user_id):
    """Convert a row from an XLS file into a series of JSON objects.

    Args:
        row (list): A row from a XLS file in list form.
        user_id (int): An integer uniquely identifying the user.

    Returns:
        list: A list of strings of JSON objects.
    """
    output = []
    for i, val in enumerate(row):
        # The first element is the number of non-zero elements, which is
        # useless to us
        if i == 0:
            continue
        # 99 is used as the NULL character
        if val == 99:
            continue
        # Otherwise element i corresponds to the rating for joke i
        current_rating = deepcopy(RATINGS)
        current_rating["user_id"] = user_id
        current_rating["joke_id"] = i
        current_rating["rating"] = val
        output.append(json.dumps(current_rating))

    return output


def block_generator(filename):
    """Generates a set of "data blocks" from the joke file.

    Each block corresponds to the data from a single joke, with the following
    form:

    block = [
        "Int:", # Joke Number
        "HTML joke",
        ...
        "End of HTML joke."
    ]

    Args:
        filename (str): The name of the file to load.

    Yields:
        list: A list of all the lines in a file relating to a single joke, in
            order.

    """
    buffer = []
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if not line:
                yield buffer
                buffer = []
            else:
                buffer.append(line)


def joke_to_json(path):
    """Convert the joke file to a list of JSON objects.

    Args:
        path (str): The location of the joke file.

    Returns:
        list: A list of JSON objects represented as strings.

    """

    output = []
    for block in block_generator(path):
        current_joke = deepcopy(JOKES)

        # The first line is the number, with a colon at the end
        current_joke["joke_id"] = int(block[0][:-1])

        # All remaining lines are the text of the joke as HTML
        text = '\n'.join(block[1:])
        current_joke["joke_text"] = bs4.BeautifulSoup(text, "lxml").get_text().strip()

        output.append(json.dumps(current_joke))

    return output


if __name__ == "__main__":

    import argparse

    # Set up command line flag handling
    parser = argparse.ArgumentParser(
            description="Transform the Jester datasets to JSON",
        )
    parser.add_argument(
            'joke_file',
            type=str,
            help="the file containing the jokes, normally 'jester_items.dat'",
            )
    parser.add_argument(
            'ratings_file',
            type=str,
            nargs='+',
            help="the xls files containing the ratings",
            )
    parser.add_argument(
            '-o',
            '--output-directory',
            type=str,
            action="store",
            help="the directory to save the output JSON files, by default the current directory",
            default="./",
            )

    args = parser.parse_args()

    # Parse the ratings
    ratings_output = args.output_directory + "/jester_ratings.json"
    with open(ratings_output, 'w') as f:
        for i, row in enumerate(iter_rows(*args.ratings_file)):
            for rating in row_to_jsons(row, i):
                f.write(rating + "\n")

    # Parse the jokes
    joke_output = args.output_directory + "/jester_jokes.json"
    with open(joke_output, 'w') as f:
        for j in joke_to_json(args.joke_file):
            f.write(j + "\n")
