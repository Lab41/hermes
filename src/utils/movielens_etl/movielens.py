#!/usr/bin/env python

"""Translate the MovieLens 20M dataset to JSON.

This script takes the four files from the MovieLens 20M dataset and
rewrites each row as a JSON object. This makes it very easy to work
with in Spark.

Attributes:
    RATINGS (dict): A dictionary that stores information from all of
        the rating actions taken by the users in the dataset. The
        variables are as follows:
            - user_id (int): A unique identifier for each user.
            - movie_id (int): A unique identifier for each movie.
            - rating (float): The user's star rating for a movie,
                from 0.5 to 5.0 stars with steps of half a star.
            - timestamp (int): Seconds since the Unix epoch.
    TAGS (dict): A dictionary that stores information from all of the
        tagging actions taken by the users in the dataset. The
        variables are as follows:
            - user_id (int): A unique identifier for each user.
            - movie_id (int): A unique identifier for each movie.
            - tag (str): The tag applied by the user.
            - timestamp (int): Seconds since the Unix epoch.
    MOVIES (dict): A dictionary that stores information about each
        movie in the dataset. The variables are as follows:
            - movie_id (int): A unique identifier for each movie.
            - imdb_id (bool): The unique identifier for each movie
                used by IMDb.
            - tmdb_id (bool): The unique identifier for each movie
                used by TMDb (The Movie Database).
            - title (str): The title of the movie.
            - year (int): The year included with the title in
                movies.csv. It is not necessarily the year the movie
                was released, sometimes it is the DVD release date,
                or some other date. If date is important to your
                model, you should probably use an alternative source
                to get it.
            - genre_action (bool): True if the movie was categorized
                by MovieLens as an action movie, else false.
            - genre_adventure (bool): True if the movie was
                categorized by MovieLens as an adventure movie, else
                false.
            - genre_animation (bool): True if the movie was
                categorized by MovieLens as an animated movie, else
                false.
            - genre_childrens (bool): True if the movie was
                categorized by MovieLens as a children's movie, else
                false.
            - genre_comedy (bool): True if the movie was categorized
                by MovieLens as a comedy, else false.
            - genre_crime (bool): True if the movie was categorized
                by MovieLens as an crime movie, else false.
            - genre_documentary (bool): True if the movie was
                categorized by MovieLens as a documentary, else false.
            - genre_drama (bool): True if the movie was categorized
                by MovieLens as a drama, else false.
            - genre_fantasy (bool): True if the movie was categorized
                by MovieLens as an fantasy movie, else false.
            - genre_filmnoir (bool): True if the movie was
                categorized by MovieLens as film-noir, else false.
            - genre_horror (bool): True if the movie was categorized
                by MovieLens as a horror movie, else false.
            - genre_musical (bool): True if the movie was categorized
                by MovieLens as a musical, else false.
            - genre_mystery (bool): True if the movie was categorized
                by MovieLens as a mystery, else false.
            - genre_romance (bool): True if the movie was categorized
                by MovieLens as a romance movie, else false.
            - genre_scifi (bool): True if the movie was categorized
                by MovieLens as an sci-fi movie, else false.
            - genre_thriller (bool): True if the movie was
                categorized by MovieLens as a thriller movie, else
                false.
            - genre_war (bool): True if the movie was categorized by
                MovieLens as a war movie, else false.
            - genre_western (bool): True if the movie was categorized
                by MovieLens as a western, else false.
            - genre_none (bool): True if the movie was not
                categorized by MovieLens.

"""

from copy import deepcopy
import json
import csv


# JSON rating object
RATINGS = {
    "user_id": None,
    "movie_id": None,
    "rating": None,
    "timestamp": None,
}

# JSON tag object
TAGS = {
    "user_id": None,
    "movie_id": None,
    "tag": None,
    "timestamp": None,
}

# JSON movie object
MOVIES = {
    "movie_id": None,
    "imdb_id": None,
    "tmdb_id": None,
    "title": None,
    "year": None,
    "genre_action": False,
    "genre_adventure": False,
    "genre_animation": False,
    "genre_childrens": False,
    "genre_comedy": False,
    "genre_crime": False,
    "genre_documentary": False,
    "genre_drama": False,
    "genre_fantasy": False,
    "genre_filmnoir": False,
    "genre_horror": False,
    "genre_musical": False,
    "genre_mystery": False,
    "genre_romance": False,
    "genre_scifi": False,
    "genre_thriller": False,
    "genre_war": False,
    "genre_western": False,
    "genre_none": False,
}


def ratings_to_json_20m(ratings_csv, output_directory):
    """Convert the ratings.csv file to a file containing a collection of JSON
    objects for the 20M dataset.

    Args:
        - ratings_csv (str): The ratings file.
        - output_directory (str): The directory to write the output
            file to.

    Returns:
        None

    """
    with open(ratings_csv, 'rb') as csv_file:
        with open(output_directory + "/movielens_20m_ratings.json", 'w') as out:
            reader = csv.reader(csv_file)
            next(reader, None)  # Skip the header
            for line in reader:
                row = deepcopy(RATINGS)
                row['user_id'] = int(line[0])
                row['movie_id'] = int(line[1])
                row['rating'] = float(line[2])
                row['timestamp'] = int(line[3])
                row_str = json.dumps(row)
                out.write(row_str + '\n')
                del row


def ratings_to_json_10m(ratings_csv, output_directory, output_file="movielens_10m_ratings.json"):
    """Convert the ratings.csv file to a file containing a collection of JSON
    objects for the 10M dataset.

    Args:
        - ratings_csv (str): The ratings file.
        - output_directory (str): The directory to write the output
            file to.
        - output_file (str,optional): The name of the file to write, by default
            "movielens_10m_ratings.json"

    Returns:
        None

    """
    with open(ratings_csv, 'rb') as csv_file:
        with open(output_directory + "/" + output_file, 'w') as out:
            for line in csv_file:
                line = line.split("::")
                row = deepcopy(RATINGS)
                row['user_id'] = int(line[0])
                row['movie_id'] = int(line[1])
                row['rating'] = float(line[2])
                row['timestamp'] = int(line[3])
                row_str = json.dumps(row)
                out.write(row_str + '\n')
                del row

def ratings_to_json_1m(ratings_csv, output_directory):
    """ Same as ratings_to_json_10m() with output_file hardcoded to
    "movielens_1m_ratings.json".

    """
    return ratings_to_json_10m(ratings_csv, output_directory, output_file="movielens_1m_ratings.json")


def tags_to_json_20m(tags_csv, output_directory):
    """Convert the tags.csv file to a file containing a collection of JSON
    objects for the 20M dataset.

    Args:
        - tags_csv (str): The tags file.
        - output_directory (str): The directory to write the output
            file to.

    Returns:
        None

    """
    with open(tags_csv, 'rb') as csv_file:
        with open(output_directory + "/movielens_20m_tags.json", 'w') as out:
            reader = csv.reader(csv_file)
            next(reader, None)  # Skip the header
            for line in reader:
                row = deepcopy(TAGS)
                row['user_id'] = int(line[0])
                row['movie_id'] = int(line[1])
                row['tag'] = line[2]
                row['timestamp'] = int(line[3])
                row_str = json.dumps(row)
                out.write(row_str + '\n')
                del row


def tags_to_json_10m(tags_csv, output_directory, output_file="movielens_10m_tags.json"):
    """Convert the tags.csv file to a file containing a collection of JSON
    objects for the 1M and 10M datasets.

    Args:
        - tags_csv (str): The tags file.
        - output_directory (str): The directory to write the output
            file to.
        - output_file (str,optional): The name of the file to write, by default
            "movielens_10m_tags.json"

    Returns:
        None

    """
    with open(tags_csv, 'rb') as csv_file:
        with open(output_directory + "/" + output_file, 'w') as out:
            for line in csv_file:
                line = line.split('::')
                row = deepcopy(TAGS)
                row['user_id'] = int(line[0])
                row['movie_id'] = int(line[1])
                row['tag'] = line[2]
                row['timestamp'] = int(line[3])
                row_str = json.dumps(row)
                out.write(row_str + '\n')
                del row


def tags_to_json_1m(tags_csv, output_directory):
    """ Same as tags_to_json_10m() with output_file hardcoded to
    "movielens_1m_tags.json".

    """
    return tags_to_json_10m(tags_csv, output_directory, output_file="movielens_1m_tags.json")


def extract_title_and_year(orig_title, encoding=None):
    """Extract the title and year from the title provided in
    movies.csv.

    The titles provided in movies.csv is of the form:

        Movie Name (Year)

    Where the movie name portion may contain multiple parenthetical
    pieces. The Year portion, if it exists, is always the last
    parenthetical part.

    Args:
        - orig_title (str): The title string from the data.
        - encoding (str): The encoding of the file, often "UTF-8" or "latin-1".
            If None, than a normal Python string is used.


    Returns:
        (return_title, return_year) (str, int): Returns the title of
            the movie, and the year extracted from orig_title.
            return_year is None if no year was included in
            orig_title.

    """
    # A few of the titles are surrounded with quotes
    stripped_title = orig_title.strip('"')

    # We need to find the year, which is contained in the last set of
    # parenthesizes.
    paren_location = orig_title.rfind('(')

    # Found a parenthesis
    if paren_location > -1:
        if encoding is None:
            tmp_title = orig_title[:paren_location].strip()
        else:
            tmp_title = unicode(orig_title[:paren_location].strip(), encoding=encoding)
        tmp_year = orig_title[paren_location:]
        # Check that it looks like a year
        if len(tmp_year) == 6 \
        and (tmp_year[1] == "1" or tmp_year[1] == "2"):
            # Date found, parse and return
            out_year = int(tmp_year.strip('()'))
            return (tmp_title, out_year)

        else:
            # Looks like it doesn't so the parenthetical is probably
            # an alternate title, hence set no date and return the
            # original title
            return (orig_title, None)

    # Not found
    else:
        return (orig_title, None)


def set_genres(genre_string, row):
    """Set genre values in a movie object given a genre string.

    MovieLens passes genres in a string of the form:
    "genre1|genre2|...". This function takes that string and sets the
    correct fields in the movie object. For example, the string
    "Sci-Fi|Children" would yield and object with "genre_scifi" set
    to True, and "genre_childrens" set to True.

    In some cases MovieLens does not indicate any genre, and instead
    sets the string to "(no genres listed)". In this case, the
    special "genre_none" field is set to True.

    Returns:
        row (dict): The movie object that was passed in. Returning is
            not strictly necessary as the object is passed in by
            reference.

    """
    # Sometimes the genres are unset, so we return
    if genre_string == "(no genres listed)":
        row['genre_none'] = True
        return row
    # Otherwise we need to find genres
    genre_map = {
        "Action": "genre_action",
        "Adventure": "genre_adventure",
        "Animation": "genre_animation",
        "Children": "genre_childrens",
        "Comedy": "genre_comedy",
        "Crime": "genre_crime",
        "Documentary": "genre_documentary",
        "Drama": "genre_drama",
        "Fantasy": "genre_fantasy",
        "Film-Noir": "genre_filmnoir",
        "Horror": "genre_horror",
        "Musical": "genre_musical",
        "Mystery": "genre_mystery",
        "Romance": "genre_romance",
        "Sci-Fi": "genre_scifi",
        "Thriller": "genre_thriller",
        "War": "genre_war",
        "Western": "genre_western",
        }
    for genre, key in genre_map.iteritems():
        if genre in genre_string:
            row[key] = True

    return row


def movies_to_json_20m(movies_csv, links_csv, output_directory):
    """Convert the movies.csv and links.csv files to a file
    containing a collection of JSON objects for the 20M dataset.

    Args:
        - movies_csv (str): The movies file.
        - links_csv (str): The links file.
        - output_directory (str): The directory to write the output
            file to.

    Returns:
        None

    """
    # Cache the link results
    link_cache = {}
    with open(links_csv, 'rb') as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)  # Skip the header
        for line in reader:
            movie_id = int(line[0])
            # Sometimes these extra IDs aren't set, so if we can't convert them
            # it is because they are empty
            try:
                imdb_id = int(line[1])
            except ValueError:
                imdb_id = None
            try:
                tmdb_id = int(line[2])
            except ValueError:
                tmdb_id = None

            link_cache[movie_id] = (imdb_id, tmdb_id)

    # Process the movie results and join with the links
    with open(movies_csv, 'rb') as csv_file:
        with open(output_directory + "/movielens_20m_movies.json", 'w') as out:
            reader = csv.reader(csv_file)
            next(reader, None)  # Skip the header
            for line in reader:
                row = deepcopy(MOVIES)
                row['movie_id'] = int(line[0])
                (row['imdb_id'], row['tmdb_id']) = link_cache[row['movie_id']]
                (row['title'], row['year']) = extract_title_and_year(line[1])
                row = set_genres(line[2], row)
                row_str = json.dumps(row)
                out.write(row_str + '\n')
                del row


def movies_to_json_10m(movies_csv, output_directory, output_file="movielens_10m_movies.json", encoding=None):
    """Convert the movies.csv files to a file
    containing a collection of JSON objects for the 10M dataset.

    Args:
        - movies_csv (str): The movies file.
        - output_directory (str): The directory to write the output
            file to.
        - output_file (str,optional): The name of the file to write, by default
            "movielens_10m_movies.json"
        - encoding (str,optional): The encoding of the file, often "UTF-8" or "latin-1".
            If None, than a normal Python string is used.

    Returns:
        None

    """
    # Process the movie results and join with the links
    with open(movies_csv, 'rb') as csv_file:
        with open(output_directory + "/" + output_file, 'w') as out:
            for line in csv_file:
                line = line.split('::')
                row = deepcopy(MOVIES)
                row['movie_id'] = int(line[0])
                (row['title'], row['year']) = extract_title_and_year(line[1], encoding=encoding)
                row = set_genres(line[2], row)
                row_str = json.dumps(row)
                out.write(row_str + '\n')
                del row


def movies_to_json_1m(movies_csv, output_directory):
    """ Same as movies_to_json_10m() with output_file hardcoded to
    "movielens_1m_movies.json" and encoding hardcoded to "latin-1".

    """
    return movies_to_json_10m(movies_csv, output_directory, output_file="movielens_1m_movies.json", encoding="latin-1")
