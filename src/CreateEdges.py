'''
Created on Nov 5, 2015

@author: Sid
'''

from mrjob.job import MRJob
from mrjob.step import MRStep 
from itertools import combinations as cmb

class MRCreateEdges(MRJob):
    def steps(self):
        return [
                MRStep(mapper=self.MapInput, reducer=self.ReduceListByDate),
                MRStep(mapper=self.MapPairs, reducer=self.ReduceCountDays)
                ]
        
    def MapInput(self, key, line):
        row = line.split(',')
        if row[0] != 'ID':
            yield row[1], row[0]
        
    def ReduceListByDate(self, key, value):
        yield key, list(value)
        
    def MapPairs (self, key, valueList):
        locPairs = list(cmb(sorted(map(int,valueList)),2))
        for eachPair in locPairs:
            yield eachPair, 1
            
    def ReduceCountDays(self, key, num):
        #weightCount = sum(num)
        #if weightCount > 4:
        yield key, sum(num)

if __name__ == '__main__':
    MRCreateEdges.run()