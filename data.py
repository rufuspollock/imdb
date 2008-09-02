'''Analyse IMDB data.

Requires imdbpy: <http://imdbpy.sourceforge.net/>

On Debian/Ubuntu you can do:

    $ aptitude install python-imdbpy

However to get scripts (which we need) seems you have to install the tarball.
(I used IMDb-3.6).

Docs at: http://imdbpy.sourceforge.net/

Loaded data using http://imdbpy.sourceforge.net/docs/README.sqldb.txt
'''
import os
import urllib

import pylab
from sqlalchemy import *
import simplejson as sj

import imdb

urlbase = 'ftp://ftp.fu-berlin.de/pub/misc/movies/database/'
fns = [ 'movies.list.gz', 'actors.list.gz', 'actresses.list.gz',
        'release-dates.list.gz', 'running-times.list.gz', 'countries.list.gz']
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
    # create toload so we can do incremental loading
    # cache = 'toload'
    cmd = 'imdbpy2sql.py -d %s -u %s' % (cache, dburi)
    # os.system(cmd)
    print cmd

def analyse_via_imdb_package():
    # does not seem very flexible
    import imdb
    i = imdb.IMDb('sql', dburi)
    movies = i.search_movie('Raiders of the Lost Ark')
    m = movies[0]
    print m['title']
    print m.__dict__

# db like
_l = lambda x: '%' + x + '%'

class Analyser(object):
    def __init__(self):
        metadata = MetaData()
        metadata.bind = dburi
        engine = metadata.bind
        # titles != movies since some stuff is e.g. videogames
        self.titles = Table('title', metadata, autoload=True)
        self.kinds = Table('kind_type', metadata, autoload=True)
        self.roles = Table('role_type', metadata, autoload=True)
        self.infotypes = Table('info_type', metadata, autoload=True)
        self.persons = Table('name', metadata, autoload=True)
        self.castinfo = Table('cast_info', metadata, autoload=True)
        # titles -> title_id
        # castinfo -> movie_id (title), person_id
        # person_id -> name.id
        self.imdb = imdb.IMDb('sql', dburi)

    def table_info(self):
        import pprint
        print self.titles.c
        print self.kinds.c
        print self.roles.c
        print self.infotypes.c
        print self.persons.c
        print self.castinfo.c
        print
        print '==== movie types'
        result = self.kinds.select().execute()
        for r in result:
            print r
        print
        print '==== roles'
        result = self.roles.select().execute()
        for r in result:
            print r
        print
        print '==== info types'
        result = self.infotypes.select().execute()
        for r in result:
            print r

    def get_year_production(self, year):
        query = self.titles.count()
        query = query.where(self.titles.c.kind_id == 1)
        query = query.where(self.titles.c.production_year == year)
        # results = select([func.count(self.titles.c.id)], and_(self.titles.c.kind_id == 1,
        #    self.titles.c.production_year==1950)).execute()
        # print results.fetchall()[0][0]
        count = query.execute().fetchall()[0][0]
        return count
    
    def production_summary(self):
        results = []
        for year in range(1900, 2007):
            v = [year, self.get_year_production(year)]
            print v
            results.append(v)
        return results

    def production_summary_info(self):
        fn1 = 'production_summary.js'
        if not os.path.exists(fn1):
            data = self.production_summary()
            analyser.dump(data, fn1)
        else:
            data = sj.load(file(fn1))
        data = zip(*data)
        pylab.bar(data[0], data[1])
        pylab.grid()
        pylab.savefig(fn1 + '.png')

    def search_movies(self, title):
        query = self.titles.select()
        query = query.where(self.titles.c.kind_id == 1)
        query = query.where(self.titles.c.title.ilike(_l(title)))
        out = query.execute()
        return out

    def get_cast(self, movie_id):
        m = self.imdb.get_movie(movie_id)
        print m['countries']
        return m['cast']

    def get_cast_via_sqlalchemy(self, movie_id): 
        query = self.titles.select()
        query = query.where(self.titles.c.id == movie_id)
        query = query.column(self.castinfo).select_from(
                self.titles.join(
                    self.castinfo, self.castinfo.c.movie_id==self.titles.c.id
                    )
                ).apply_labels()
        print query
        return query.execute()
    
    def dump(self, data, fn):
        fo = file(fn, 'w')
        sj.dump(data, fo, sort_keys=True, indent=4)

class TestAnalyser(object):
    blahfn = 'adfafadsfjadsf.js'
    analyser = Analyser()

    def setUp(self):
        if os.path.exists(self.blahfn):
            os.remove(self.blahfn)

    def test_dump(self):
        data = {1: 'abc', '1': 'def', 'data': [[1,2], [3,4]]}
        self.analyser.dump(data, self.blahfn)
        assert os.path.exists(self.blahfn)

    def _test_search_movies(self):
        res = self.analyser.search_movies('Indiana Jones')
        res = res.fetchall()
        m = res[0]
        # nb: first one was called raiders of the lost ark
        assert m.title == 'Indiana Jones and the Last Crusade'
        assert m.production_year == 1989
        assert len(res) == 4, len(res)

    def test_get_cast(self):
        # Indiana Jones
        movie_id = 234009
        info = self.analyser.get_cast(movie_id)
        assert info[0]['name'] == 'Harrison Ford'
        assert info[0].personID == 277020

    def test_get_cast_via_sqlalchemy(self):
        # Indiana Jones
        movie_id = 234009
        q = self.analyser.get_cast_via_sqlalchemy(movie_id)
        info = q.fetchall()
        print info[0]
        # role type 1 is actor
        assert info[0].cast_info_role_id == 1

def main():
    retrieve()
    load()
    analyser = Analyser()
    # analyser.table_info()

if __name__ == '__main__':
    # analyser = Analyser()
    main()

