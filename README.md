# hermes
Hermes is Lab41's foray into recommender systems. It explores how to choose a recommender system for a new application by analyzing the performance of multiple recommender system algorithms on a variety of datasets.

It also explores how recommender systems may assist a software developer of data scientist find new data, tools, and computer programs.

The Wiki associated with this project has details on many [references](http://github.com/Lab41/hermes/wiki/References) that we utilized when implementing this framework. It also details the [datasets](http://github.com/Lab41/hermes/wiki/Datasets) used in this base framework, as well as some [resources](https://github.com/Lab41/hermes/wiki/Training-Materials) to help you get started in recommender systems and Spark.

For tips on how to get started, see the wiki page: [Running Hermes](https://github.com/Lab41/hermes/wiki/Running-Hermes).

##Blog Overviews
There are a number of blog articles that we produced during the course of this project.  They include:
|       |      |
|-------|------|
| [Join the Hermes Running Club](http://www.lab41.org/join-the-hermes-running-club/) | March 2016 |
| [Python2Vec: Word Embeddings for Source Code](http://www.lab41.org/py2vec/) | March 2016|
| [TPS Report for Recommender Systems? Traditional Performance Metrics](http://www.lab41.org/tps-report-for-recommender-systems-yeah-that-would-be-great/) | March 2016|
| [Recommender Systems - It's Not All About the Accuracy ](http://www.lab41.org/recommender-systems-its-not-all-about-the-accuracy/) | January 2016|
| [The Nine Must-Have Datasets for Investigating Recommender Systems](http://www.lab41.org/py2vec/) | February 2016|
| [Recommending Recommendation Systems (project intro)](http://www.lab41.org/recommending-recommendation-systems/) | December 2015|

## visualization
We are trying varied tools and concepts to visualize the results of this project.

### boku

* `conda install bokeh`
* from top-level hermes folder `$bokeh serve src/results/hermes_run_view.py`
* view in browser at `http://localhost:5006/hermes_run_view`


### d3

* `easy_install web.py`
* from viz folder `$python app.py`
* view in browser from location:port displayed in terminal
