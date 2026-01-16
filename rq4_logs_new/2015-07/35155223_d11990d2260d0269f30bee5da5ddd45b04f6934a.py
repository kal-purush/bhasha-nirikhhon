#script (python)

from gringo import *
from pprint import pprint

def onModel(model):
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

def main(ctr):
    
    time = 1
    frameCnt = 0
    
    ctr.ground([("base", [])])

    cmd = raw_input("> ")
    while cmd != "quit":
        if cmd == "next":
            ctr.add("time{}".format(time), ["t"], "time(t).")
            ctr.ground([("base", []), ("time{}".format(time), [time])])
            time += 1
        elif cmd == "show":
            ret = ctr.solve([], onModel)
        else:
            frame = eval(cmd)
            print frame
            frameName = frame[0]
            frameSource = frame[1]
            frameLP = "ticker({},{},{},{}).".format(frameCnt,frameSource,frameName,time)
            
            for attName, attVal in frame[2].iteritems():
                frameLP += "attribute({},{},{},{},{},{}).".format(frameCnt,frameSource,frameName,attName,attVal,time)
            
            lpName = "p{}{}".format(time,frameCnt)
            ctr.add(lpName, [], frameLP)
            ctr.ground([(lpName, [])])
            
            frameCnt += 1
        
        cmd = raw_input("> ")

#end.