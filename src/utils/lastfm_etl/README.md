# Last.fm

The [last.fm](http://www.last.fm/) HetRec 2011 Data comes last.fm and was
compiled by Ignacio Fernández-Tobías, Iván Cantador, and Alejandro Bellogín
and is available [here](http://grouplens.org/datasets/hetrec-2011/).

More information about the data is available in Cantador et al.:

> 2nd Workshop on Information Heterogeneity and Fusion in Recommender Systems
> (HetRec 2011). I. Cantod, P Brusilovsky, T. Kuflik. Proceedings of the 5th
> ACM conference on Recommender systems.

## Converting the Raw Data

Following files are required for the ratings:

- artists.dat (referred to as the artist file)
- tags.dat (referred to as the tags file)
- user_friends.dat (referred to as the friends graph file)
- user_taggedartists.dat (referred to as the applied tags file)
- user_artists.dat (referred to as the play counts file)

The converting script is run as follows:

```bash
./lastfm.py artists.dat tags.dat user_friends.dat user_taggedartists.dat user_artists.dat
```

## Dependencies

All dependencies should come by default in Python 2.7.
