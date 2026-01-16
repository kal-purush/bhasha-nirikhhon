import math
import random
import time
import numpy as np
import pandas as pd
import pylab as plt
import bokeh
from bokeh.models import ColumnDataSource, Line, Legend, HoverTool, LinearColorMapper
from bokeh.palettes import Category10
from bokeh.plotting import figure
from mesa import Agent, Model
from mesa.time import RandomActivation
from mesa.space import MultiGrid
from mesa.datacollection import DataCollector
import enum

#copied in parts from https://dmnfarrell.github.io/bioinformatics/abm-mesa-python

class State(enum.IntEnum):
    SUSCEPTIBLE = 0
    INFECTED = 1
    REMOVED = 2
    DEAD = 3
    WALL = 4


class InfectionModel(Model):
    """A model for infection spread."""

    def __init__(self, N=10, width=10, height=10, ptrans=0.5,
                 death_rate=0.02, recovery_days=21,
                 recovery_sd=7, infected_start=0.02, surrounded = True, walls = [], bed_capacity=1, death_untreated=1.5,
                 tries_to_move=1, chance_to_stay=0, stay_if_infected=0):
        self.num_agents = N
        self.recovery_days = recovery_days
        self.recovery_sd = recovery_sd
        self.ptrans = ptrans
        self.death_rate = death_rate
        self.grid = MultiGrid(width, height, True)
        self.schedule = RandomActivation(self)
        self.running = True
        self.infected_agents = 0
        self.dead_agents = 0
        self.dead_agents_history = []
        self.walls = walls
        self.bed_capacity = math.ceil(N * bed_capacity)
        self.additional_death_rate = 0
        self.death_untreated = death_untreated
        self.tries_to_move = tries_to_move
        self.chance_to_stay = chance_to_stay
        self.stay_if_infected = stay_if_infected
        if surrounded:
            walls += [(0, y) for y in range(height)] + [(width-1, y) for y in range(height)]
            walls += [(x, 0) for x in range(width)] + [(x, height-1) for x in range(width)]
        self.not_on_each_other = self.num_agents + len(self.walls) <= self.grid.width * self.grid.height
        #create walls(also agents)
        i = -1
        for (x,y) in walls:
            a = MyAgent(i, self)
            i -= 1
            self.grid.place_agent(a, (x,y))
            a.state = State.WALL
        # Create agents
        number_of_infected_at_start = math.ceil(self.num_agents * infected_start)
        for i in range(self.num_agents):
            a = MyAgent(i, self)
            self.schedule.add(a)
            # Add the agent to a random grid cell
            x = random.randrange(self.grid.width)
            y = random.randrange(self.grid.height)
            if self.not_on_each_other:
                while len(self.grid.get_cell_list_contents([(x,y)])) != 0:
                    x = self.random.randrange(self.grid.width)
                    y = self.random.randrange(self.grid.height)
            self.grid.place_agent(a, (x, y))
            #make some agents infected at start
            if i < number_of_infected_at_start:
                a.state = State.INFECTED
                a.recovery_time = self.get_recovery_time()
        self.datacollector = DataCollector(
            agent_reporters={"State": "state"})

    def get_recovery_time(self):
        return int(self.random.normalvariate(self.recovery_days,self.recovery_sd))

    def step(self):
        # self.additional_death_rate = self.death_untreated if self.infected_agents > self.bed_capacity else 1
        self.additional_death_rate = self.infected_agents/self.bed_capacity * self.death_untreated if self.infected_agents > self.bed_capacity else 1
        self.datacollector.collect(self)
        self.schedule.step()
        self.dead_agents_history += [self.dead_agents]



class MyAgent(Agent):
    """ An agent in an epidemic model."""
    def __init__(self, unique_id, model):
        super().__init__(unique_id, model)
        self.age = self.random.normalvariate(20,40)
        self.state = State.SUSCEPTIBLE
        self.infection_time = 0

    def status(self):
        """Check infection status"""

        if self.state == State.INFECTED:
            dead_chance = self.model.death_rate * self.model.additional_death_rate
            dead = dead_chance > random.random()
            if dead:
                self.state = State.DEAD
                # self.model.dead_agents += [self]
                self.model.dead_agents += 1
                self.model.schedule.remove(self)  # dead -> none
                self.model.grid.remove_agent(self)
                self.model.infected_agents -= 1
            else:
                t = self.model.schedule.time-self.infection_time
                if t >= self.recovery_time:
                    self.state = State.REMOVED
                    self.model.infected_agents -= 1

    def move(self):
        """Move the agent"""
        if self.state is State.DEAD:
            return
        possible_steps = self.model.grid.get_neighborhood(
            self.pos,
            moore=True,
            include_center=False)

        new_position = self.random.choice(possible_steps)
        i = 0
        stay_in_place = self.model.chance_to_stay
        if self.state is State.INFECTED:
            stay_in_place += self.model.stay_if_infected
        if stay_in_place > random.random():
            new_position = self.pos
        elif self.model.not_on_each_other:
            # if len(self.model.grid.get_cell_list_contents([new_position])) != 0:
            #     new_position = self.pos
            while i < self.model.tries_to_move and len(self.model.grid.get_cell_list_contents([new_position])) > 0:
                new_position = self.random.choice(possible_steps)
                i += 1
            if i >= self.model.tries_to_move:
                new_position = self.pos
        self.model.grid.move_agent(self, new_position)


    def contact(self):
        """Find close contacts and infect if infected"""
        if self.state is not State.INFECTED:
            return
        neighbours = self.model.grid.get_cell_list_contents(self.model.grid.get_neighborhood(
            self.pos,
            moore=True,
            include_center=True))
        for other in neighbours:
            if other.state is State.SUSCEPTIBLE:
                if self.model.ptrans > random.random():
                    other.state = State.INFECTED
                    other.infection_time = self.model.schedule.time
                    other.recovery_time = self.model.get_recovery_time()
                    self.model.infected_agents += 1

    def step(self):
        self.status()
        self.move()
        self.contact()

def get_column_data(model):
    """pivot the model dataframe to get states count at each step"""
    agent_state = model.datacollector.get_agent_vars_dataframe()
    X = pd.pivot_table(agent_state.reset_index(),index='Step',columns='State',aggfunc=np.size,fill_value=0)
    labels = ['Susceptible','Infected','Removed','Dead']
    X.columns = labels[:len(X.columns)]
    return X

def plot_states_bokeh(model,title='', wideness=100):

    X = get_column_data(model)
    if model.schedule.time > 0:
        X = X.assign(Dead=(model.dead_agents_history))
        if model.bed_capacity < model.num_agents:
            X = X.assign(Beds=(model.bed_capacity))
    X = X.reset_index()
    source = ColumnDataSource(X)
    colors = {"Susceptible": "#2ca02c", "Infected": "#ff7f0e", "Removed": "#1f77b4", "Dead": "black", "Wall": "White", "Beds": "Red"}
    items=[]
    p = figure(width=700,height=500,tools=[],title=title,x_range=(0,wideness),y_range=(0,model.num_agents))
    for c in X.columns[1:]:
        line = Line(x='Step',y=c, line_color=colors[c],line_width=3,line_alpha=.8,name=c)
        glyph = p.add_glyph(source, line)
        items.append((c,[glyph]))

    p.xaxis.axis_label = 'Step'
    p.add_layout(Legend(location='center_right',
                items=items))
    p.background_fill_color = "#e1e1ea"
    p.background_fill_alpha = 0.5
    p.legend.label_text_font_size = "10pt"
    p.title.text_font_size = "15pt"
    p.toolbar.logo = None
    return p

def grid_values(model):
    """Get grid cell states"""

    agent_counts = np.zeros((model.grid.width, model.grid.height))
    w=model.grid.width
    df=pd.DataFrame(agent_counts)
    for cell in model.grid.coord_iter():
        agents, x, y = cell
        c=None
        for a in agents:
            c = a.state
        df.iloc[x,y] = c
    return df

def plot_cells_bokeh(model):

    agent_counts = np.zeros((model.grid.width, model.grid.height))
    w=model.grid.width
    df=grid_values(model)
    df = pd.DataFrame(df.stack(), columns=['value']).reset_index()
    columns = ['value']
    x = [(i, "@%s" %i) for i in columns]
    hover = HoverTool(
        tooltips=x, point_policy='follow_mouse')

    colors = ("#2ca02c", "#ff7f0e", "#1f77b4", "black", "white")
    mapper = LinearColorMapper(palette=colors, low=0, high=4)
    p = figure(width=550,height=500, tools=[hover], x_range=(-1,w), y_range=(-1,w))
    p.rect(x="level_0", y="level_1", width=1, height=1,
       source=df,
       fill_color={'field':'value', 'transform': mapper},
       line_color='black')
    p.background_fill_color = "black"
    p.grid.grid_line_color = None
    p.axis.axis_line_color = None
    p.toolbar.logo = None
    return p