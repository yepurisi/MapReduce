'''
Created on Oct 28, 2015

@author: Sid
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

regExp = re.compile(r'[\w]+')

class MRWordCount(MRJob):
    
    def steps(self):
        return [
                MRStep(mapper=self.mapper_get_words, reducer=self.reducer_count_words), 
                MRStep(mapper=self.mapper_flip_keys, reducer=self.reducer_sorted_freq)
                ]
    
    def mapper_get_words(self, key, line):
        words = regExp.findall(line)
        for eachWord in words:
            yield eachWord.lower(), 1
            
    def reducer_count_words(self, word, count):
        yield word, sum(count)
        
    def mapper_flip_keys(self, word, count):
        yield '%04d'%int(count), word
        
    def reducer_sorted_freq(self, count, words):
        for eachWord in words:
            yield eachWord, count

if __name__ == '__main__':
    MRWordCount.run()