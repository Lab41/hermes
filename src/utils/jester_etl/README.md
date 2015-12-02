# Jester Data

The Jester Data comes from Ken Goldberg at UC Berkeley, and can be found
[here](http://eigentaste.berkeley.edu/dataset/).

More information about the data is available in Goldber et. al:

> [Eigentaste: A Constant Time Collaborative Filtering
> Algorithm](http://www.ieor.berkeley.edu/~goldberg/pubs/eigentaste.pdf). Ken
> Goldberg, Theresa Roeder, Dhruv Gupta, and Chris Perkins. Information
> Retrieval, 4(2), 133-151. July 2001.

## Converting the Raw Data

Following files are required for the ratings:

- jester-data-1.xls from [jester_dataset_1_1.zip](http://eigentaste.berkeley.edu/dataset/jester_dataset_1_1.zip)
- jester-data-2.xls from [jester_dataset_1_2.zip](http://eigentaste.berkeley.edu/dataset/jester_dataset_1_2.zip)
- jester-data-3.xls from [jester_dataset_1_3.zip](http://eigentaste.berkeley.edu/dataset/jester_dataset_1_3.zip)
- jesterfinal151cols.xls from [jester_dataset_2+.zip](http://eigentaste.berkeley.edu/dataset/jester_dataset_3.zip)

Additionally, to get the content of the jokes, the following data is needed:

- jester_items.dat from [jester_dataset_2.zip](http://eigentaste.berkeley.edu/dataset/jester_dataset_2.zip)

The converting script is run as follows:

```bash
./jester.py jester_items.dat jester-data-1.xls jester-data-2.xls jester-data-3.xls jesterfinal151cols.xls
```

## Dependencies

### Mac OSX

On MAC OS, you might need to install:

#### xlrd (tested on version 0.9.4)

```bash
// download xlrd from https://pypi.python.org/pypi/xlrd
// cd xlrd-0.9.4
python setup.py install
```

#### libxml2

```bash
brew install libxml2
brew link libxml2 --force
```

#### libxslt

```bash
brew install libxslt
brew link libxslt --force
```

#### lxml

```bash
pip install lxml
```

#### BeautifulSoup

```bash
pip install beautifulsoup4
```

Tested on xlrd version 0.9.4, beautifulsoup4.
