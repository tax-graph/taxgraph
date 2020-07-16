# taxgraph
This repository contains the code for building the knowledge graph TaxGraph. For more information on TaxGraph
see [taxgraph.informatik.uni-mannheim.de](http://taxgraph.informatik.uni-mannheim.de/).

A dump of the knowledge graph can be downloaded [here](https://zenodo.org/record/3946462).

## Building the knowledge graph
Five path variables have to be specified in `createRDF.py` before running the file to build the knowledge graph.

`path_lei_data`: This path points to a Golden Copy File containing information about legal entities. These files are published
by [GLEIF](https://www.gleif.org/) and can be downloaded from
[here](https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/). Download the _LEI-CDF v2.1_ file.
The code expects the file to be in CSV format. Our knowledge graph was build with the file from 2019-10-09 08:00.

`path_relationship_data`: This path points to a Golden Copy File containing information about the relationships between legal
entities. These files are published by [GLEIF](https://www.gleif.org/) and can be downloaded from
[here](https://www.gleif.org/en/lei-data/gleif-golden-copy/download-the-golden-copy#/). Download the _RR-CDF v1.1_ file.
The code expects the file to be in CSV format. Our knowledge graph was build with the file from 2019-10-09 08:00.

`path_wikidata_cities`: This path points to a CSV file containing combinations of wikidata entity ID, postal code and label.
A compressed version of the file that we used can be found under `data/wikidataCityData/wikidata_cities.csv.gz`.

`path_additonal_data`: This path points to a file containing additional data retrieved from the
[World Bank](https://data.worldbank.org/), the [OECD](https://stats.oecd.org/) and [Wikidata](https://www.wikidata.org/). This
file can be created by running `createAdditionalDataSets.py`. The file that we used for building our version of the
knowledge graph can be found under `data/additionalData/2020-03-17_00:56:06_df.pkl`.

`graph_storage_folder`: This path points to the folder in which to store the final knowledge graph as an RDF file.

## Memory footprint
The build process has a high memory footprint, as most of the data processing is performed in-memory. We build the knowledge
graph on a machine with 32 GB of memory. By optimizing the code and rewriting the data processing to be performed on disk, it
should be possible to reduce the memory footprint by a lot.
