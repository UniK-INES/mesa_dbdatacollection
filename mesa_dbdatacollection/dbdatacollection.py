'''
Created on 25.04.2022

@author: Sascha Holzhauer

- reads DB config from given config file
- creates tables and prepared statements as required during initialisation
- adds rows to tables

'''
from mesa.datacollection import DataCollector
from functools import partial
from operator import attrgetter
import configparser
import os
#import time
from datetime import datetime
import pandas as pd
import types

from sqlalchemy import Table, Column, ForeignKey, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import engine_from_config
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text, MetaData

import d6tstack.utils


Base = declarative_base()

class RunInfo(Base):
    '''
    RunInfo class to store run IDs with creation time
    '''
    __tablename__ = 'runs'
    id = Column(Integer, primary_key=True)
    creation = Column(DateTime)
    
class DbDataCollector(DataCollector):
    '''
    classdocs
    
    Offers functionality to store collected data to database.
    Extends mesa.datacollection.DataCollector
    '''


    def __init__(self,
                 configfile="../config/resultdb.cfg",
                 model_reporters=None, 
                 agent_reporters=None,
                 tables=None):
        '''
        Constructor
                
        :param configfile: config file for db parameter
        :param model_reporters: mesa model reporters
        :param agent_reporters: mesa agent reporters
        :param tables: additional tables
        '''
        
        configParser = configparser.RawConfigParser() 
        configFilePath = os.path.join(os.path.dirname(__file__), configfile)
        configParser.read(configFilePath)
        
        self.configDb = dict(configParser.items('db'))
        self.cacheParams = dict(configParser.items('caching'))
        
        self.maxRunId = None
        
        self.engine = engine_from_config(self.configDb)     

        self.meta = MetaData(bind=self.engine)
        self.meta.reflect()
        
        self.cachedrows = {}
        
        DBSession = sessionmaker(bind=self.engine)
        self.session = DBSession()
        
        super().__init__(model_reporters, agent_reporters, tables)
        
    def addRunId(self):
        '''
        Add an incremented run ID to tabel runs.
        Create table if not existing
        '''
        
        # Check table and find highest run ID in table
        if self.engine.dialect.has_table(self.engine.connect(), "runs"):
            result = self.session.execute(text("SELECT MAX(id) FROM runs"))
            self.maxRunId = result.first()[0]
        else:
            RunInfo.__table__.create(self.engine)
        
        # increment run ID
        if self.maxRunId == None:
            self.maxRunId = 1
        else:
            self.maxRunId += 1
            
        # add new run ID with timestamp
        runinfo = RunInfo(id=self.maxRunId, creation= datetime.now()) #time.strftime('%Y-%m-%d %H:%M:%S'))
        self.session.add(runinfo)
        self.session.commit()
        
        self.con = self.engine.connect()
                 
    def close(self):
        '''
        Close db session
        '''
        if self.session:
            self.session.close()
                    

    def _new_table(self, table_name, table_columns):
        """
        Add a new table that objects can write to.

        :param table_name: Name of the new table.
        :type table_name: string
        :param table_columns: List of columns to add to the table.
        :type table_columns: list

        """
        
        # only store columns to create table later on with correct data types (?)
        table = Table(table_name, self.meta, *table_columns, extend_existing=True)
        table.create(checkfirst=True)
        
        self.tables[table_name] = table
        self.cachedrows[table_name] = list()

    def _record_agents(self, model):
        """
        Record agents data in a mapping of functions and agents.
        :param model: mesa model
        :type model. mesa.Model
        """
        rep_funcs = self.agent_reporters.values()
        if all([hasattr(rep, "attribute_name") for rep in rep_funcs]):
            prefix = ["model.datacollector.maxRunId", "model.schedule.steps", "unique_id"]
            attributes = [func.attribute_name for func in rep_funcs]
            get_reports = attrgetter(*prefix + attributes)
        else:

            def get_reports(agent):
                # mesa increments step right after agent loop
                _prefix = (self.maxRunId, agent.model.schedule.steps - 1, agent.unique_id)
                reports = tuple(rep(agent) for rep in rep_funcs)
                return _prefix + reports

        agent_records = map(get_reports, model.schedule.agents)
        return agent_records
    
    def collect(self, model):
        '''
        Collect model and agent reporters
        :param model: mesa model
        :type model. mesa.Model
        '''

        self.addRunId()
            
        if self.model_reporters:
            self.model_vars = {}
            # fill prepared statement
            # mesa increments step right after agent loop
            self.model_vars['step'] = model.schedule.steps - 1  
            for var, reporter in self.model_reporters.items():
                # Check if Lambda operator
                if isinstance(reporter, types.LambdaType):
                    self.model_vars[var] = reporter(model)
                # Check if model attribute
                elif isinstance(reporter, partial):
                    self.model_vars[var] = reporter(model)
                # Check if function with arguments
                elif isinstance(reporter, list):
                    self.model_vars[var] = reporter[0](*reporter[1])
                else:
                    # Why decorator?
                    self.model_vars[var] = self._reporter_decorator(reporter)
        
            data = {'runID': self.maxRunId}
            data.update(self.model_vars)
            df = pd.DataFrame([data])

            self.pd_to_db(df, 'model')
            
        if self.agent_reporters:
            agent_records = self._record_agents(model)
            
            # store agents' records
            df = pd.DataFrame(agent_records)
            df.columns = ["runID", "step", "agentId"] + \
                    [func for func in self.agent_reporters.keys()]
            
            self.pd_to_db(df, 'agents')

    
    def add_table_row(self, table_name, row, ignore_missing=False):
        """
        Add a row dictionary to a specific table.

        :param table_name: Name of the table to append a row to.
        :type table_name: string
        :param row: A dictionary of the form {column_name: value...}
        :type row: dict
        :param ignore_missing: If True, fill any missing columns with Nones;
                            if False, throw an error if any columns are missing
        :type ignore_missing: boolean
        """
        if table_name not in self.tables:
            raise Exception("Table " + table_name + " does not exist.")
    
        for column in self.tables[table_name].columns:
            if column not in row and not ignore_missing:
                raise Exception("Could not insert row with missing column")
        
        d = {'runID': self.maxRunId}
        d.update(row)
        self.cachedrows[table_name].append(d)
       
        if len(self.cachedrows[table_name]) % int(self.cacheParams['cachenum.tables']) == 0:
            self.con.execute(self.tables[table_name].insert(), self.cachedrows[table_name])
            self.cachedrows[table_name] = list()

    
    def add_table_rows(self, table_name, rows):
        """
        Add a row dictionary to a specific table.

        :param table_name: Name of the table to append a row to.
        :type table_name: string
        :param rows: rows to store
        :type rows: pandas.DataFrame
        """
        
        if table_name not in self.tables:
            raise Exception("Table does not exist.")
        
        df = {'runID': self.maxRunId}
        df.update(pd.DataFrame(rows))
        
        self.pd_to_db(df, table_name)
        
        
    def pd_to_db(self, df, tablename):
        '''
        Insert pandas dataframe to SQL DB using d6stack if DB URI is supported.
        Otherwise use SQLalchemy.
        
        :param df:
        :param tablename:
        '''
        if 'psycopg2' in self.configDb['sqlalchemy.url']:
            d6tstack.utils.pd_to_psql(df, self.configDb['sqlalchemy.url'],
            tablename, if_exists='append',sep=';')
            
        elif 'mysql+mysqlconnector' in self.configDb['sqlalchemy.url']:
            d6tstack.utils.pd_to_mysql(df, self.configDb['sqlalchemy.url'], tablename, if_exists='append',sep=';')
            
        elif 'mssql+pymssql' in self.configDb['sqlalchemy.url']:
            d6tstack.utils.pd_to_mssql(df, self.configDb['sqlalchemy.url'], tablename, if_exists='append',sep=';')
        
        else:
            df.to_sql(tablename, self.engine, index= False, if_exists='append')
            