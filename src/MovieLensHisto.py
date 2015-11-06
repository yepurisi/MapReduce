'''
Created on Oct 28, 2015
@author: Sid
'''

from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        userID, movieID, rating, timeStamp = line.split('\t')
        yield rating, 1
    
    def reducer(self, rating, count):
        yield rating, sum(count)

if __name__ == '__main__':
    MRRatingCounter.run()