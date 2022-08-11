'''
Created on 30.05.2022

@author: Sascha Holzhauer
'''

from example.model import ConwaysGameOfLife

from sqlalchemy import event
from sqlalchemy.engine import Engine
import time

    
if __name__ == '__main__':
    
    golmodel = ConwaysGameOfLife(width = 100, height=100, usedb=True)
    for i in range(0,10):
        golmodel.step()
    