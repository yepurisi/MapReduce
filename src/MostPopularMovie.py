'''
Created on Oct 29, 2015

@author: Sid
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularMovie(MRJob):
    
    def configure_options(self):
        super(MRMostPopularMovie, self).configure_options()
        self.add_file_option('--items', help='data/ml-100k/u.item.txt')
    
    def steps(self):
        return [
                MRStep(mapper=self.mapper_movieIds, reducer_init=self.reducer_init, reducer=self.reducer_counts),
                MRStep(reducer=self.reducer_get_max)
                ]

    def mapper_movieIds(self, key, line):
        userID, movieID, rating, timeStamp = line.split('\t')
        yield movieID, 1
        
    def reducer_init(self):
        
        self.movieNames = {}
        
        with open("/Programming/MapReduce/data/ml-100k/u.item.txt", encoding='utf-8') as f:
            for eachLine in f:
                lineData = eachLine.split('|')
                self.movieNames[lineData[0]] = lineData[1]
    
    def reducer_counts(self, movie, counts):
        yield None, (sum(counts), self.movieNames[movie])
        
    def reducer_get_max(self, movie, ratings):
        yield max(ratings)

if __name__ == '__main__':
    MRMostPopularMovie.run()