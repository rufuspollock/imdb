# Analysing Movies with IMDB

Data grabbing, analysis etc is currently all in data.py.

## Movies and Actors: Mapping the Internet Movie Database

http://ivl.slis.indiana.edu/km/pub/2007-herr-movieact.pdf

## IMDB Graph Drawing Competition

http://www.ul.ie/gd2005/dataset.html

Evolving-Graph Drawing Competition

The challenge of drawing large evolving graphs can be addressed in various ways. Any visualizations based on the contest data, including animations, static images, subgraphs and derivations of the contest graph, are welcome as submission. In addition to the visualizations, we encourage contestants to submit supplemental material, such as background relevance of the graph, case studies, concepts, algorithms, experiments, structural results, that address the problem of visualizing this type of data in a meaningful way.
Contest Data

A real-world data set is provided that is based on the Internet Movie Database.

The graph is a bipartite graph where each node either corresponds to an actor or to a movie. There is an edge between a movie and each actor of the movie.

Moreover, the data contain the following attributes at nodes:

    * "movie" indicating if node corresponds to a movie (type boolean)
    * "name" indicating name of movie resp. actor (type string)
    * year of the movie (type int); attribute is 0 if node is an actor or year is not known
    * genre of the movie (type string) 

Download

The graph is available in GraphML format (compressed with bzip2):

    * imdb.graphml.bz2 (26MB) 
