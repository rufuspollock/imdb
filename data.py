'''Analyse IMDB data.

Requires imdbpy: <http://imdbpy.sourceforge.net/>

On Debian/Ubuntu you can do:

    $ aptitude install python-imdbpy

However to get scripts (which we need) seems you have to install the tarball.
(I used IMDb-3.6).
'''
import os
import urllib

urlbase = 'ftp://ftp.fu-berlin.de/pub/misc/movies/database/'
fns = [ 'movies.list.gz', 'actors.list.gz', 'actresses.list.gz' ]
cache = os.path.abspath('cache')

def retrieve():
    if not os.path.exists(cache):
        os.makedirs(cache)
    for fn in fns:
        url = urlbase + fn
        dest = os.path.join(cache, fn)
        if not os.path.exists(dest):
            print 'Retrieving %s to %s' % (url, dest)
            urllib.urlretrieve(url, dest)
        else:
            print 'Skipping %s' % url

# follow http://imdbpy.sourceforge.net/docs/README.sqldb.txt
dburi = 'postgres://rgrp:pass@localhost/imdb'
def load():
    cmd = 'imdbpy2sql.py -d %s -u %s' % (cache, dburi)
    # os.system(cmd)
    print cmd

def analyse_via_imdb():
    # does not seem very flexible
    import imdb
    i = imdb.IMDb('sql', dburi)
    movies = i.search_movie('Indiana Jones and the Ark')
    m = movies[0]
    print m['title']
    print m.__dict__

from sqlalchemy import *
def analyse():
    metadata = MetaData()
    metadata.bind = dburi
    engine = metadata.bind
    # titles != movies since some stuff is e.g. videogames
    titles = Table('title', metadata, autoload=True)
    kinds = Table('kind_type', metadata, autoload=True)
    result = kinds.select().execute()
    # movie has kind_id = 1

    def get_year_production(year):
        query = titles.count()
        query = query.where(titles.c.kind_id == 1)
        query = query.where(titles.c.production_year == year)
        # results = select([func.count(titles.c.id)], and_(titles.c.kind_id == 1,
        #    titles.c.production_year==1950)).execute()
        # print results.fetchall()[0][0]
        count = query.execute().fetchall()[0][0]
        return count
    for year in range(1900, 1980):
        print year, get_year_production(year)

    # persons = Table('person_info', metadata, autoload=True)
    # castinfo = Table('cast_info', metadata, autoload=True)
    # titles -> title_id
    # person_info -> person_id
    # castinfo -> movie_id (title), person_id

if __name__ == '__main__':
    retrieve()
    load()
    analyse()
