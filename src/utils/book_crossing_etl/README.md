# Book-Crossing Data

The Book-Crossing data comes from Cai-Nicolas Ziegler of IIF and can be found
[here](http://www2.informatik.uni-freiburg.de/~cziegler/BX/).

More information about the data is available in Ziegler et al.:

> [Improving Recommendation Lists Through Topic
> Diversification](http://www2.informatik.uni-freiburg.de/~dbis/Publications/05/WWW05.html).
> Cai-Nicolas Ziegler, Sean M. McNee, Joseph A. Konstan, Georg Lausen;
> Proceedings of the 14th International World Wide Web Conference (WWW '05),
> May 10-14, 2005, Chiba, Japan. To appear.

## Converting the Raw Data

Following files are required for the ratings:

- BX-Book-Ratings.csv (referred to as the ratings file)
- BX-Users.csv (referred to as the users file)
- BX-Books.csv (referred to as the books file)

All three files are available in the [CSV dump file
(BX-CSV-Dump.zip)](http://www2.informatik.uni-freiburg.de/~cziegler/BX/BX-CSV-Dump.zip).

The converting script is run as follows:

```bash
./bookcrossing.py BX-Book-Ratings.csv BX-Users.csv BX-Books.csv
```

**NOTE**: The converting script prunes users and books in the following
manner:

1. Any entry in the rating file that does not match to a book in the books
   file or a user in the user file is removed.
2. Any user in the users file is removed if they do not have at least one
   rating saved from the rating file.

In this manner invalid reviews and invalid (and inactive) users are removed
before the conversion to JSON.

## Dependencies

All dependencies should come by default in Python 2.7.
