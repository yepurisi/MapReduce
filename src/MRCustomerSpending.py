'''
Created on Oct 28, 2015

@author: Sid
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCustomerSpending(MRJob):
    
    def steps(self):
        return [
                MRStep(mapper=self.mapper_customer, reducer=self.reducer_summed), 
                MRStep(mapper=self.mapper_flip_keys, reducer=self.reducer_output)
                ]
    
    def mapper_customer(self, key, line):
        customer, item, amount = line.split(',')
        yield customer, float(amount)
        
    def reducer_summed(self, customer, amounts):
        yield customer, sum(amounts)
        
    def mapper_flip_keys(self, customer, totalSpent):
        yield '%04.02f'%float(totalSpent), customer
        
    def reducer_output(self, amount, customers):
        for customer in customers:
            yield customer, float(amount)

if __name__ == '__main__':
    MRCustomerSpending.run()