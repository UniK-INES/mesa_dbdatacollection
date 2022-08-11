'''
Created on 07.07.2022

@author: Sascha Holzhauer
'''
from example.model import ConwaysGameOfLife
import os

def run_model_dataframe():
    golmodel = ConwaysGameOfLife(width = 100, height=100, usedb = False)
    for i in range(0,10):
        golmodel.step()
    
    os.makedirs("./tests/temp", exist_ok=True)
    md = golmodel.datacollector.get_model_vars_dataframe()
    md.to_csv('./tests/temp/model_vars.csv')
    ad = golmodel.datacollector.get_agent_vars_dataframe()
    ad.to_csv('./tests/temp/agent_vars.csv')
    td = golmodel.datacollector.get_table_dataframe("neighbours")
    td.to_csv('./tests/temp/neighbours.csv')
    
    return True

def run_model_db():
    golmodel = ConwaysGameOfLife(width = 100, height=100, usedb = True)
    for i in range(0,10):
        golmodel.step()
    
    return True
    
def test_dataframe(benchmark):
    # benchmark something
    result = benchmark(run_model_dataframe)
    assert result == True
 
def test_db(benchmark):
    # benchmark something
    result = benchmark(run_model_db)
    assert result == True   
    