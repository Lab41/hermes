#!/bin/bash

# List of repos to download
repos=(
    "https://github.com/matplotlib/matplotlib"
    "https://github.com/scikit-learn/scikit-learn"
    "https://github.com/numpy/numpy"
    "https://github.com/pydata/pandas"
    "https://github.com/django/django"
    "https://github.com/scipy/scipy"
    "https://github.com/mitsuhiko/flask"
    "https://github.com/kennethreitz/requests"
    "https://github.com/ansible/ansible"
    "https://github.com/getsentry/sentry"
    "https://github.com/scrapy/scrapy"
    "https://github.com/mailpile/Mailpile"
    "https://github.com/apenwarr/sshuttle"
    "https://github.com/saltstack/salt"
    "https://github.com/samuelclay/NewsBlur"
    "https://github.com/beetbox/beets"
    "https://github.com/SublimeCodeIntel/SublimeCodeIntel"
)

# Download and ETL the repos
for repo in ${repos[@]}; do
    ./repo_to_json.py "$repo" /tmp/code_etl/
done
