#!/usr/bin/python
#
# author:
# 
# date: 
# description:
#

# for using the program one need to install the gringo python lib
import gringo
from somewhere import getASPFactsFromFrames # thats clayton's function

SOLVERESULT = None

# TODO: replace print by storing informations
def onModel(model):
    # this function is just a ref call - so we use the global SOLVERESULT 
    # for storing the result of the onModel function
    global SOLVERESULT
    
    atoms = model.atoms(Model.ATOMS)
    events = getFluents(atoms, "ticker")
    states = getFluents(atoms, "holds" )
    attributes = getFluents(atoms, "attribute")
    for time in range(len(states)):
        print "{}".format(time)
        if time in events:
            for e in events[time]:
                print "[e]  {}".format(e)
                for att in attributes:
                    if att[0] == e: print "---  {}: {}".format(att[1], att[2])
        print "[s] "
        for s in states[time]:
            print "     {}".format(s)

def getFluents(atoms, name):
    if name == "holds":
        fluents = {}
        for (lit,time) in [atom.args() for atom in atoms if atom.name() == name]:
            if time not in fluents:
                fluents[time] = [lit]
            else:
                fluents[time].append(lit)
        return fluents
    elif name == "ticker":
        fluents = {}
        for (id,frame,time) in [atom.args() for atom in atoms if atom.name() == name and len(atom.args()) == 3]:
            if time not in fluents:
                fluents[time] = [(id,frame)]
            else:
                fluents[time].append((id,frame))
        return fluents
    elif name == "attribute":
        return [atom.args() for atom in atoms if atom.name() == name and len(atom.args()) == 4]

def loadScences(ctr):
    scenes = [  "actors", "chance", "competition", "field", "foul", 
                "goal", "lineup", "match", "mixup", "motion", "move",
                "one_on_one", "pass", "shot", "state_of_match", 
                "substitution"
    ]
    # load each sence into the controler
    # that path is choosen from the root of the project
    for scene in scenes:
        ctr.load("asp/scenes/{}.lp".format(scene))

def solve(frames):
    # Clayton's function for translating frames into asp facts
    frameFacts = getASPFactsFromFrames(frames)
    # initializing the asp controller obj
    ctr = gringo.Control()
    # load the logic prorams into the controller
    ctr.load("meta.enc.lp")
    loadScences(ctr)
    # ground the lp
    ctr.ground([("base", [])])
    # solve it, while on finding a model, the function onModel is called
    ret = ctr.solve([], onModel)