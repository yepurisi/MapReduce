'''
Created on Oct 28, 2015

@author: Sid
'''

from mrjob.job import MRJob

class FriendsByAge(MRJob):
    
    def mapper(self, key, line):
        id, name, age, friends = line.split(',')
        yield age, int(friends)
        
    def reducer(self, age, count):
        total = 0; numOfAge = 0
        for x in count:
            total = total + x
            numOfAge = numOfAge + 1
        yield age, total/numOfAge

if __name__ == '__main__':
    FriendsByAge.run()