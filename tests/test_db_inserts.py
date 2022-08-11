'''
Created on 08.07.2022

@author: Sascha Holzhauer
'''

import pytest
from mesa_dbdatacollection.dbdatacollection import DbDataCollector, RunInfo
from sqlalchemy import Column, Integer, MetaData

import os
import configparser

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import engine_from_config
from sqlalchemy.sql import exists

from example.model import ConwaysGameOfLife

Session = sessionmaker()

@pytest.fixture(scope='module')
def connection():
    configParser = configparser.RawConfigParser() 
    configParser.read(os.path.dirname(os.path.abspath(__file__)) + "/config/resultdb.cfg")
    configDb = dict(configParser.items('db'))
    engine = engine_from_config(configDb)     
    connection = engine.connect()
    yield connection
    connection.close()

@pytest.fixture(scope='function')
def session(connection):
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()

def setupmodel():
    width = 100
    height = 100
    model = ConwaysGameOfLife(width = width, height=height, usedb=True)
    return model


class TestRunID:
    """
    Test generation of run ID table entry
    """

    datacollector = None
    
    @pytest.fixture()
    def setupdb(self, session):
        meta = MetaData(bind=session.connection())
        for table in reversed(meta.sorted_tables):
            session.execute(f"TRUNCATE {table.name} RESTART IDENTITY CASCADE;")
    
        # os.remove(os.path.dirname(os.path.abspath(__file__)) + "/temp/sqlite.db")
        self.datacollector = DbDataCollector(
                configfile = os.path.dirname(os.path.abspath(__file__)) + "/config/resultdb.cfg",
                tables={"testdata": [
                    Column("step", Integer),
                    Column("unique_id", Integer),
                    Column("agentdata", Integer)
                    ]}
                )
        yield

    def test_runInfo(self, setupdb, session):
        self.datacollector.addRunId()
        assert session.query(RunInfo).one()
        
        
class TestAgentReporters:
    """
    Test agent reporter features of DbDataCollector
    """

    datacollector = None
    
    @pytest.fixture()
    def setupdb(self): 
        self.datacollector = DbDataCollector(
                configfile = os.path.dirname(os.path.abspath(__file__)) + "/config/resultdb.cfg",
                agent_reporters={"isAlive": "isAlive",
                                 "x": lambda a: a.x,
                                 "y": lambda a: a.y,
                                 },
                )

    #@pytest.mark.skip()
    def test_agentReporter(self, setupdb, session):
        model = setupmodel()
        model.step()
        self.datacollector.collect(model)
        result = session.execute('SELECT COUNT(*)  AS numrows FROM agents WHERE `step` = 0')
        assert result.fetchone()['numrows'] == model.grid.width * model.grid.height
        
        
class TestModelReporters:
    """
    Test model reporter features of DbDataCollector
    """
    
    datacollector = None
    
    @pytest.fixture()
    def setupdb(self): 
        self.datacollector = DbDataCollector(
                configfile = os.path.dirname(os.path.abspath(__file__)) + "/config/resultdb.cfg",
                model_reporters={"number of agents":  lambda m: m.schedule.get_agent_count(),
                                 "number of alive agents": "numAliveAgents"
                                 },
                )

    #@pytest.mark.skip()
    def test_modelReporter(self, setupdb, session):
        model = setupmodel()
        model.step()
        self.datacollector.collect(model)
        result = session.execute('SELECT COUNT(*)  AS numrows FROM model WHERE `step` = 0')
        assert result.fetchone()['numrows'] == 1
        result = session.execute('SELECT * FROM model WHERE `step` = 0')
        assert result.fetchone()._asdict()['runID'] == 1


class TestTableReporters:
    """
    Test arbitrary table generation features of DbDataCollector
    """

    datacollector = None
    
    @pytest.fixture()
    def setupdb(self): 
        self.datacollector = DbDataCollector(
                configfile = os.path.dirname(os.path.abspath(__file__)) + "/config/resultdb.cfg",
                tables={"testdata": [
                    Column("step", Integer),
                    Column("unique_id", Integer),
                    Column("alive_neighbors", Integer)
                    ]}
                )

    #@pytest.mark.skip()
    def test_tableReporter(self, setupdb, session):
        model = setupmodel()
        model.step()
        self.datacollector.collect(model)
        for row in model.neighbours:
            self.datacollector.add_table_row("testdata", row, ignore_missing=True)
        result = session.execute('SELECT COUNT(*)  AS numrows FROM testdata')
        assert result.fetchone()['numrows'] == model.grid.width * model.grid.height
        