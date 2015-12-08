#!/usr/bin/env python

import argparse

from movielens import ratings_to_json_1m, tags_to_json_1m, movies_to_json_1m

# Set up command line flag handling
parser = argparse.ArgumentParser(
    description="Transform the MovieLens 1M dataset CSV files to JSON",
)
parser.add_argument(
    'movies_csv',
    type=str,
    help="the CSV file containing movie data",
)
parser.add_argument(
    'ratings_csv',
    type=str,
    help="the CSV file containing rating data",
)
parser.add_argument(
    '-o',
    '--output-directory',
    type=str,
    action="store",
    help="the directory to save the output JSON files, by default the current directory",
    default="./",
)


# Run only if this script is being called directly
if __name__ == "__main__":

    args = parser.parse_args()

    ratings_to_json_1m(args.ratings_csv, args.output_directory)
    movies_to_json_1m(args.movies_csv, args.output_directory)
