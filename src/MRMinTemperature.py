'''
Created on Oct 28, 2015

@author: Sid
'''

from mrjob.job import MRJob

class MRMinTemperature(MRJob):
    
    def convertTempToFH(self, cel):
        celsius = float(cel)/10.0
        return (celsius*1.8 + 32.0)        
    
    def mapper(self, key, line):
        location, date, ttype, temp, x, y, z, w = line.split(',')
        if (ttype == 'TMIN'):
            ftemp = self.convertTempToFH(temp)
            yield location, ftemp
        
    def reducer(self, location, listTemp):
        yield location, min(listTemp)
        

if __name__ == '__main__':
    MRMinTemperature.run()