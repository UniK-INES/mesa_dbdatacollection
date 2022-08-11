'''
Created on 30.05.2022

@author: Sascha Holzhauer
'''

from example.model import ConwaysGameOfLife
import logging

from sqlalchemy import event
from sqlalchemy.engine import Engine
import time

logging.basicConfig()
logger = logging.getLogger("dbdatacollect")
logger.setLevel(logging.DEBUG)

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())
    logger.debug("Start Query: %s", statement)

@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    logger.debug("Query Complete!")
    logger.debug("Total Time: %f", total)
    
if __name__ == '__main__':
    golmodel = ConwaysGameOfLife(width = 50, height=50)
    
    logging.basicConfig(level=logging.DEBUG)
    
    logger = logging.getLogger('sqlalchemy')
    logger.setLevel(level="INFO")
    # logging.Logger.setLevel(level="INFO")

    for i in range(0,3):
        golmodel.step()
    