'''
Created on Oct 31, 2015

@author: Sid
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularSuperHero(MRJob):
    
    def configure_options(self):
        super(MRMostPopularSuperHero, self).configure_options()
        self.add_file_option('--names', help='data/marvel-names.txt')
        
    def steps(self):
        return [
                MRStep(mapper=self.mapperCounts, reducer=self.reducerSumCounts),
                MRStep(mapper=self.mapForMax, mapper_init=self.mapForMaxInit, reducer=self.reduceMaxCount)
                ]
        
    def mapperCounts(self, key, line):
        inStr = line.split()
        yield int(inStr[0]), len(inStr) - 1
        
    def reducerSumCounts(self, key, value):
        yield key, sum(value)
    
    def mapForMaxInit(self):
        self.heroNames = {}
        with open('marvel-names.txt', 'r') as f:
            for eachRow in f:
                inString = eachRow.split()
                heroId = inString[0]
                self.heroNames[int(heroId)] = inString[1] 
        
    def mapForMax(self, hero, count):
        heroName = self.heroNames[hero]
        yield None, (count, heroName)
    
    def reduceMaxCount(self, key, value):
        yield max(value)

if __name__ == '__main__':
    MRMostPopularSuperHero.run()