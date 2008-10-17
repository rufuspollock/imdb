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
import time

import pylab
from sqlalchemy import *
import simplejson as sj

import imdb

urlbase = 'ftp://ftp.fu-berlin.de/pub/misc/movies/database/'
fns = [ 'movies.list.gz', 'actors.list.gz', 'actresses.list.gz',
        'release-dates.list.gz', 'running-times.list.gz',
        'countries.list.gz', 
        'business.list.gz', # movie revenues, costs etc
        ]
cache = os.path.abspath('cache')
# follow http://imdbpy.sourceforge.net/docs/README.sqldb.txt
dburi = 'postgres://rgrp:pass@localhost/imdb'

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
        self.movieinfo = Table('movie_info', metadata, autoload=True)
        # self.persons = Table('name', metadata, autoload=True)
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
        print self.movieinfo.c
        # print self.persons.c
        print self.castinfo.c
        def showenum(table):
            print
            print '==== ', table.name
            result = table.select().execute()
            for r in result:
                print r
        showenum(self.kinds)
        showenum(self.roles)
        showenum(self.infotypes)

    def list_countries(self):
        mi = self.movieinfo
        # q = self.movieinfo.select([mi.c.info])
        q = select([mi.c.info])
        q = q.where(mi.c.info_type_id == 8)
        q = q.distinct()
        print q
        # results are dumped in countries.js
        return q.execute()

    def production(self):
        q = select([self.titles.c.production_year,
            func.count(self.titles.c.id)])
        q = q.where(self.titles.c.kind_id == 1)
        q = q.where(self.titles.c.production_year != None)
        q = q.group_by(self.titles.c.production_year)
        q = q.order_by(self.titles.c.production_year)
        # q = q.where(self.titles.c.production_year == year)
        return q

    def production_by_country(self, country):
        q = self.production()
        q = q.select_from(self.titles.join(
            self.movieinfo, self.movieinfo.c.movie_id==self.titles.c.id)
            ).apply_labels()
        # runtime is id 8
        q = q.where(self.movieinfo.c.info_type_id == 8)
        q = q.where(self.movieinfo.c.info == country)
        # print q
        return q

    def production_by_running_time(self, year):
        q = select([
            self.titles.c.id,
            self.titles.c.production_year,
            self.movieinfo.c.info])
        q = q.select_from(self.titles.join(
            self.movieinfo, self.movieinfo.c.movie_id==self.titles.c.id)
            ).apply_labels()
        q = q.where(self.titles.c.kind_id == 1)
        q = q.where(self.titles.c.production_year == year)
        q = q.order_by(self.titles.c.production_year)
        # runtime is id 1
        q = q.where(self.movieinfo.c.info_type_id == 1)
        total = 0.0
        for row in q.execute():
            runtime = 0
            try:
                runtime = float(row[2])
            except:
                try:
                    #  Singapore:5 
                    runtime = row[2].split(':')[1]
                    runtime = float(runtime)
                except:
                    print 'Could not process: %s' % row
            total += row[1]
        return total
    
    def search_movies(self, title):
        query = self.titles.select()
        query = query.where(self.titles.c.kind_id == 1)
        query = query.where(self.titles.c.title.ilike(_l(title)))
        out = query.execute()
        return out

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

    def plot_production(self, country='all', show_total=True):
        if show_total:
            # fn1 = 'production_summary.js'
            # if not os.path.exists(fn1):
            #     total = self.production().execute().fetchall()
            #     total = [ [x[0], x[1]] for x in total ]
            #     self.dump(total, fn1)
            # else:
            #     total = sj.load(file(fn1))
            total = a.production().execute().fetchall()
            total = zip(*total)
            pylab.bar(total[0], total[1])
        if country != 'all':
            cdata = a.production_by_country(country).execute().fetchall()
            cdata = zip(*cdata)
            pylab.bar(cdata[0], cdata[1], color='r')
        pylab.grid()
        fn = 'production_%s.png' % country.replace(' ', '_').lower()
        fn = str(fn)
        pylab.savefig(fn)

    def running_time_summary(self):
        fn = os.path.abspath('running_times.js')
        if not os.path.exists(fn):
            results = []
            st = time.time()
            for year in range(1897, 2008):
                output = self.production_by_running_time(year)
                results.append([year, output])
            end = time.time()
            print end - st
            self.dump(results, fn)
        else:
            results = sj.load(open(fn))
        return results
    
    def dump(self, data, fn):
        fo = file(fn, 'w')
        sj.dump(data, fo, sort_keys=True, indent=4)


class Plotter(object):
    def __init__(self):
        self.a = Analyser()

    def plot_all(self):
        self.a.plot_production(u'USA')
        pylab.clf()
        # totals are so much bigger it messes up figure
        self.a.plot_production(u'India', show_total=False)
        pylab.clf()
        self.a.plot_production(u'UK', show_total=False)
        pylab.clf()
        self.a.plot_production(u'France', show_total=False)
        pylab.clf()
        self.a.plot_production(u'Germany', show_total=False)
        pylab.clf()
        self.a.plot_production(u'Hong Kong', show_total=False)

    def plot_running_times(self):
        running = self.a.running_time_summary()
        pylab.clf()
        running = zip(*running)
        # pylab.plot(running[0], running[1])
        pylab.bar(running[0], running[1])
        country = 'all'
        fn = 'production_by_times_%s.png' % country.replace(' ', '_').lower()
        pylab.savefig(fn)


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

    def test_production_by_country(self):
        out = self.analyser.production_by_country(u'USA')
        out = out.execute().fetchall()
        print out
        assert len(out) >= 120, len(out)
        out = dict(out)
        assert out[1900] == 527

    def test_get_movie(self):
        movie_id = 234009
        m = self.analyser.imdb.get_movie(movie_id)
        assert len(m['countries']) == 1
        assert m['countries'][0] == u'USA'

    def test_get_cast(self):
        # Indiana Jones
        movie_id = 234009
        m = self.analyser.imdb.get_movie(movie_id)
        info = m['cast']
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
    analyser.table_info()

def demo():
    a = Analyser()
    movie_id = 234009
    m = a.imdb.get_movie(movie_id)
    print m['countries']

    res = a.search_movies('3 hommes et un couffin')
    for r in res: print r

if __name__ == '__main__':
    # main()
    a = Analyser()
    # demo()
    plotter = Plotter()
    # plotter.plot_running_times()

