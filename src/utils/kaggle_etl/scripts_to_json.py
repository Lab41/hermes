#!/usr/bin/env python

"""Translate a kaggle kernal metadata to JSON.

This script takes a directory containing the kaggle kernal metadata files (csv files)
and converts them into a JSON object containing the same information.

The Kaggle Kernal metadata files can be downloaded from https://www.kaggle.com/kaggle/meta-kaggle
as licensed by creative commons: https://creativecommons.org/licenses/by-nc-sa/4.0/
The praimry files that are required include:

Scripts.csv - contains script information including author, and the parent script

     {
        "Id": the script id
        "AuthorUserId": the author's id
        "ScriptProjectId": the Kaggle competition id
        "CurrentScriptVersionId": the current script version
        "UrlSlug": competition name
        "TotalViews": number of script views
        "ForkParentScriptVersionId": the parent script this script was forked from
        "FirstScriptVersionId": the first script id for this script
        "IsProjectLanguageTemplate": boolean, True if the script is a template
        "ForumTopicId": if applicable a reference to the forum topic id
        "TotalVotes": number of total views
    }


ScriptVersions.csv -
    {
        "Id": id of the script version
        "DateCreated": date and time created
        "ScriptId": The script id - believed to be the link to scripts.csv
        "Title": script title
        "ScriptContent": The actual content of the script - aka the code
        "TemplateScriptVersionId": ID of the template script
        "IsChange": boolean if a change
        "ScriptLanguageId": script language id.
            2 is a python script, 8 is Ipython Notebook, and 9 is Ipython Notebook HTML
            (no noticable difference between 8 and 9)
        "TotalLines": number of lines
        "LinesInsertedFromPrevious": inserted lines
        "LinesDeletedFromPrevious": deleted lines
        "LinesChangedFromPrevious": changed lines
        "LinesUnchangedFromPrevious": unchanged lines
        "LinesInsertedFromFork": inserted lines from the fork script
        "LinesDeletedFromFork": deleted lines from the fork script
        "LinesChangedFromFork": changed lines from the fork script
        "LinesUnchangedFromFork": unchanged lines from the fork script
        "TotalVotes": total number of votes
    }


Examples:

    With a local repository:

        ./scripts_to_json.py /path/to/kaggle/files/ -o /output/directory/


"""

from copy import deepcopy
import csv
import json
import sys

# JSON Kaggle object
KAGGLE = {
    "Id": None,
    "AuthorUserId": None,
    "ScriptProjectId": None,
    "CurrentScriptVersionId": None,
    "UrlSlug": None,
    "TotalViews": None,
    "ForkParentScriptVersionId": None,
    "FirstScriptVersionId": None,
    "IsProjectLanguageTemplate": None,
    "ForumTopicId": None,
    "TotalVotes": None
}

KAGGLE_VERSION = {
    "Id": None,
    "DateCreated": None,
    "ScriptId": None,
    "Title": None,
    "ScriptContent": None,
    "TemplateScriptVersionId": None,
    "IsChange": None,
    "ScriptLanguageId": None,
    "TotalLines": None,
    "LinesInsertedFromPrevious": None,
    "LinesDeletedFromPrevious": None,
    "LinesChangedFromPrevious": None,
    "LinesUnchangedFromPrevious": None,
    "LinesInsertedFromFork": None,
    "LinesDeletedFromFork": None,
    "LinesChangedFromFork": None,
    "LinesUnchangedFromFork": None,
    "TotalVotes": None
}

def parse_kaggle_script_line(line):
    (id_, user, project, cur_script, url, total_views, fork_parent, template, forum_topic, first_script, votes) = line
    kaggle_line = deepcopy(KAGGLE)
    kaggle_line["Id"] = int(id_)
    kaggle_line["AuthorUserId"] = int(user)
    kaggle_line["ScriptProjectId"] = project
    kaggle_line["CurrentScriptVersionId"] = cur_script
    kaggle_line["UrlSlug"] = url
    kaggle_line["TotalViews"] = total_views
    kaggle_line["ForkParentScriptVersionId"] = fork_parent
    kaggle_line["IsProjectLanguageTemplate"] = template
    kaggle_line["ForumTopicId"] = forum_topic
    kaggle_line["FirstScriptVersionId"] = first_script
    kaggle_line["TotalVotes"] = int(votes)
    return kaggle_line

def parse_kaggle_script_version_line(line):
    (id_,date,script,title,content,template,change,language,lines,inserted,deleted,changed,unchanged,insertedF,deletedF,changedF,unchangedF,votes) = line
    kaggle_version_line = deepcopy(KAGGLE_VERSION)
    kaggle_version_line["Id"]= id_
    kaggle_version_line["DateCreated"]= date
    kaggle_version_line["ScriptId"]= script
    kaggle_version_line["Title"]= title
    kaggle_version_line["ScriptContent"]=content
    kaggle_version_line["TemplateScriptVersionId"]= template
    kaggle_version_line["IsChange"]= change
    kaggle_version_line["ScriptLanguageId"]= language
    kaggle_version_line["TotalLines"]= lines
    kaggle_version_line["LinesInsertedFromPrevious"]= inserted
    kaggle_version_line["LinesDeletedFromPrevious"]= deleted
    kaggle_version_line["LinesChangedFromPrevious"]= changed
    kaggle_version_line["LinesUnchangedFromPrevious"]= unchanged
    kaggle_version_line["LinesInsertedFromFork"]= insertedF
    kaggle_version_line["LinesDeletedFromFork"]= deletedF
    kaggle_version_line["LinesChangedFromFork"]= changedF
    kaggle_version_line["LinesUnchangedFromFork"]= unchangedF
    kaggle_version_line["TotalVotes"]= votes
    return kaggle_version_line


if __name__ == "__main__":

    import argparse

    # Set up command line flag handling
    parser = argparse.ArgumentParser(
            description="Transform the Kaggle Script datasets to JSON",
        )
    parser.add_argument(
            'kaggle_directory',
            type=str,
            help="the directory containing the Kaggle Script data including 'scripts.csv'",
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

    # Because our files are large we need to increase the maximum size they can be
    csv.field_size_limit(sys.maxsize)

    # First parse the script file
    with open(args.output_directory + "/scripts.json", 'w') as f:
        with open(args.kaggle_directory + "/Scripts.csv", 'r') as csvfile:
            document = csv.reader(csvfile, delimiter = ',')
            next(document)
            for row in document:
                kaggle_elem = parse_kaggle_script_line(row)
                f.write(json.dumps(kaggle_elem) + '\n')

    # Now parse the script version file
    with open(args.output_directory + "/script_versions.json", 'w') as f:
        with open(args.kaggle_directory + "/ScriptVersions.csv", 'r') as csvfile:
            document = csv.reader(csvfile, delimiter = ',')
            next(document)
            for row in document:
                kaggle_elem = parse_kaggle_script_version_line(row)
                f.write(json.dumps(kaggle_elem) + '\n')