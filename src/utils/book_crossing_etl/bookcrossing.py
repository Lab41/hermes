#!/usr/bin/env python

"""Translate the Book-Crossing dataset to JSON.

This script takes the various Book-Crossing data files and write them out as
JSON. It removes users that have no ratings as well as ratings of books that do
not exist.

Attributes:
    RATINGS (dict): A dictionary that stores information from all of
        the rating actions taken by the users in the dataset. The
        variables are as follows:
            - user_id (int): A unique identifier for each user.
            - book_id (int): A unique identifier for each book.
            - rating (int): The user's rating for a book,
                from 1 to 10. None if an implicit rating.
            - implicit (bool): True if the rating is "implicit", that is, just
                an interaction with the book instead of a numeric rating.

     BOOKS (dict): A dictionary that stores information about all the books in
        the dataset. The variables are as follows:
            - book_id (str): A unique identifier for each book.
            - title (str): The title of the book.
            - author (str): The author of the book.
            - year (int): The year the book was published.
            - publisher (str): The publisher of the book.

    USERS (dict): A dictionary that stores information about the users. The
        variables are as follows:
            - user_id (int): A unique identifier for each user.
            - location (str): A location, often of the form "city, state,
                country".
            - age (int): The age of the user in years; can be None.

"""

from copy import deepcopy
import json
import csv


# JSON rating object
RATINGS = {
    "user_id": None,
    "book_id": None,
    "rating": None,
    "implicit": None,
}

# JSON book object
BOOKS = {
    "book_id": None,
    "title": None,
    "author": None,
    "year": None,
    "publisher": None,
}

# JSON user object
USERS = {
    "user_id": None,
    "location": None,
    "age": None,
}


def convert_str(string):
    """Convert a string from 'iso-8859-1' to 'utf8'."""
    return string.decode('iso-8859-1').encode('utf8')


def iter_lines(open_file):
    """Open the Book-Crossing CSVs and return an iterator over the lines.

    Args:
        open_file: A file handle object from open().

    Retunrs:
        iterator: An iterator over each line in the file. Each line is a list,
            with string elements for each column value.

    """
    reader = csv.reader(
        open_file,
        delimiter=';',
        doublequote=False,
        escapechar="\\",
    )
    next(reader)  # Skip the header

    return reader


def parse_user_line(line):
    """Parse a line from the user CSV file.

    A line is a list of strings as follows:

        line = [
            user_id,
            location,
            age,
        ]

    Args:
        lines (list): A list of strings as described above.

    Returns:
        dict: A dictionary containing the keys "user_id", "location", and
            "age".

    """
    (user, location, age) = line
    current_user = deepcopy(USERS)
    current_user["user_id"] = int(user)
    current_user["location"] = convert_str(location)
    # Sometimes the age is "NULL", which we handle by leaving the
    # value as None
    try:
        current_user["age"] = int(age)
    except ValueError:
        pass

    return current_user


def parse_rating_line(line):
    """Parse a line from the ratings CSV file.

    A line is a list of strings as follows:

        line = [
            user_id,
            book_id,
            rating,
        ]

    Args:
        lines (list): A list of strings as described above.

    Returns:
        dict: A dictionary containing the keys "user_id", "book_id", "rating",
            and "implicit".

    """
    (user, book, rating) = line
    current_rating = deepcopy(RATINGS)
    current_rating["user_id"] = int(user)
    current_rating["book_id"] = convert_str(book)
    rating = int(rating)
    if rating == 0:
        current_rating["implicit"] = True
    else:
        current_rating["rating"] = rating

    return current_rating


def parse_book_line(line):
    """Parse a line from the book CSV file.

    A line is a list of strings as follows:

        line = [
            book,
            title,
            author,
            year,
            publisher,
            small_cover_url,
            medium_cover_url,
            large_cover_url,
        ]

    Args:
        lines (list): A list of strings as described above.

    Returns:
        dict: A dictionary containing the keys "book_id", "title", "author",
            "year", and "publisher".

    """
    # We throw out the three images from Amazon (hence the _,_,_)
    (book, title, author, year, publisher, _, _, _) = line
    current_book = deepcopy(BOOKS)
    current_book["book_id"] = convert_str(book)
    current_book["title"] = convert_str(title)
    current_book["author"] = convert_str(author)
    current_book["year"] = int(year)
    current_book["publisher"] = convert_str(publisher)

    return current_book


if __name__ == "__main__":

    import argparse

    # Set up command line flag handling
    parser = argparse.ArgumentParser(
            description="Transform the Book-Crossing datasets to JSON",
        )
    parser.add_argument(
            'ratings',
            type=str,
            help="the file containing the ratings, normally 'BX-Book-Ratings.csv'",
            )
    parser.add_argument(
            'users',
            type=str,
            help="the file containing the users, normally 'BX-Users.csv'",
            )
    parser.add_argument(
            'books',
            type=str,
            help="the file containing the books, normally 'BX-Books.csv'",
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

    # There are two cases of "bad" data that we want to remove:
    #
    # 1. Ratings that do not match to a valid user or book
    # 2. Users who have no ratings after the above rule has been applied

    # Find valid books
    valid_books = []
    book_data = []
    with open(args.books, 'rb') as csvfile:
        for line in iter_lines(csvfile):
            ret = parse_book_line(line)
            valid_books.append(ret["book_id"])
            book_data.append(ret)

    # Find valid users
    valid_users = []
    users_data = []
    with open(args.users, 'rb') as csvfile:
        for line in iter_lines(csvfile):
            ret = parse_user_line(line)
            valid_users.append(ret["user_id"])
            users_data.append(ret)

    # Save only ratings that have a valid book and a valid user. Additionally,
    # save the users and books saved to filter the books and user files later.
    valid_books = set(valid_books)
    valid_users = set(valid_users)
    rated_users = []

    with\
        open(args.ratings, 'rb') as csvfile,\
        open("implicit_ratings.json", 'w') as imp,\
        open("explicit_ratings.json", 'w') as exp:

        for line in iter_lines(csvfile):
            ret = parse_rating_line(line)
            if ret["book_id"] in valid_books and ret["user_id"] in valid_users:
                rated_users.append(ret["user_id"])
                # Separate the two types of ratings; they can both be
                # read in on Spark if the user wants both.
                if ret["implicit"]:
                    imp.write(json.dumps(ret) + '\n')
                else:
                    exp.write(json.dumps(ret) + '\n')

    # Only save users that have at least one rating saved to the ratings
    # outputs.
    rated_and_valid_users = set(rated_users)

    with open("books.json", 'w') as f:
        for ret in book_data:
            f.write(json.dumps(ret) + '\n')

    with open("users.json", 'w') as f:
        for ret in users_data:
            if ret["user_id"] in rated_and_valid_users:
                f.write(json.dumps(ret) + '\n')
