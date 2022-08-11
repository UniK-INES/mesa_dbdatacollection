from mesa import Model
from mesa.time import SimultaneousActivation
from mesa.space import Grid
from mesa.datacollection import DataCollector
from mesa_dbdatacollection.dbdatacollection import DbDataCollector
from sqlalchemy import Column, Integer
from .cell import Cell
import os


class ConwaysGameOfLife(Model):
    """
    Represents the 2-dimensional array of cells in Conway's
    Game of Life.
    """

    def __init__(self, width=50, height=50, usedb = False):
        """
        Create a new playing area of (width, height) cells.
        """

        # Set up the grid and schedule.

        # Use SimultaneousActivation which simulates all the cells
        # computing their next state simultaneously.  This needs to
        # be done because each cell's next state depends on the current
        # state of all its neighbors -- before they've changed.
        self.schedule = SimultaneousActivation(self)

        # Use a simple grid, where edges wrap around.
        self.grid = Grid(width, height, torus=True)

        # Place a cell at each location, with some initialized to
        # ALIVE and some to DEAD.
        for (contents, x, y) in self.grid.coord_iter():
            cell = Cell((x, y), self)
            if self.random.random() < 0.1:
                cell.state = cell.ALIVE
            self.grid.place_agent(cell, (x, y))
            self.schedule.add(cell)

        self.running = True
        
        # data collection
        # if usedb:
            # self.datacollector = DbDataCollector(
                # configfile = os.path.dirname(os.path.abspath(__file__)) + "/config/resultdb.cfg",
                # model_reporters={"number of agents": lambda m: m.schedule.get_agent_count()},
                # agent_reporters={"isAlive": "isAlive"},
                #
                # ## !
                # tables={"neighbours": [Column("Step", Integer),
                                          # Column("unique_id", Integer),
                                          # Column("alive_neighbors", Integer)]}
            # )
        # else:
            # self.datacollector = DataCollector(
                # model_reporters={"number of agents": lambda m: m.schedule.get_agent_count()},
                # agent_reporters={"isAlive": "isAlive"},
                #
                # ## !
                # tables={"neighbours": [Column("Step", Integer),
                                          # Column("unique_id", Integer),
                                          # Column("alive_neighbors", Integer)]}
            # )
            #
        self.neighbours = [{"Step": self.schedule.steps,
                            "unique_id" : cell.unique_id,
                            # cell.unique_id[0]*self.grid.width + cell.unique_id[1]
                            "alive_neighbors": cell.aliveneighbours}
                                    for cell in self.schedule.agents]

    def step(self):
        """
        Have the scheduler advance each cell by one step
        """
        self.schedule.step()
        
        #self.datacollector.collect(self)
        # for row in self.neighbours:
            # ## !
            # self.datacollector.add_table_row("neighbours", row, ignore_missing=True)
