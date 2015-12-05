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
    return string.decode('iso-8859-1').encode('utf8')


def iter_lines(open_file):
    reader = csv.reader(
        open_file,
        delimiter=';',
        doublequote=False,
        escapechar="\\",
    )
    next(reader)  # Skip the header

    return reader


def parse_user_line(line):
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

    valid_books = []
    book_data = []
    with open(args.books, 'rb') as csvfile:
        for line in iter_lines(csvfile):
            ret = parse_book_line(line)
            valid_books.append(ret["book_id"])
            book_data.append(ret)

    valid_users = []
    users_data = []
    with open(args.users, 'rb') as csvfile:
        for line in iter_lines(csvfile):
            ret = parse_user_line(line)
            valid_users.append(ret["user_id"])
            users_data.append(ret)

    valid_books = set(valid_books)
    valid_users = set(valid_users)
    rated_books = []
    rated_users = []
    with open(args.ratings, 'rb') as csvfile:
        with open("implicit_ratings.json", 'w') as imp:
            with open("explicit_ratings.json", 'w') as exp:
                for line in iter_lines(csvfile):
                    ret = parse_rating_line(line)
                    if ret["book_id"] in valid_books and ret["user_id"] in valid_users:
                        rated_books.append(ret["book_id"])
                        rated_users.append(ret["user_id"])
                        if ret["implicit"]:
                            imp.write(json.dumps(ret) + '\n')
                        else:
                            exp.write(json.dumps(ret) + '\n')

    rated_and_valid_books = set(rated_books)
    rated_and_valid_users = set(rated_users)

    with open("books.json", 'w') as f:
        for ret in book_data:
            if ret["book_id"] in rated_and_valid_books:
                f.write(json.dumps(ret) + '\n')

    with open("users.json", 'w') as f:
        for ret in users_data:
            if ret["user_id"] in rated_and_valid_users:
                f.write(json.dumps(ret) + '\n')
