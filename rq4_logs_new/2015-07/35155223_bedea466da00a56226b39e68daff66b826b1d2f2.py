#!/usr/bin/env python

from frameextract import *
from ticker import *
import re

class Event(object):
    
    def __init__(self):
        self.ticker = None
        self.minute = None
        self.event_id = None
        self.frame = None
        self.text = None
        self.tree = None
        self.agent = None
        self.animate_obj = None
        self.inanimate_obj = None
        self.arguments = {}
        
def previous_node(node, tree):
    for n in tree.nodes:
        if int(n.node_id) == int(node.node_id) - 1:
            return n
    
def next_node(node, tree):
    for n in tree.nodes:
        if int(n.node_id) == int(node.node_id) + 1:
            return n
        
def parent(node, tree):
    for n in tree.nodes:
        if int(n.node_id) == int(node.head):
            return n

def child(node, tree):
    for n in tree.nodes:
        if int(n.head) == int(node.node_id):
            return n

        
def get_agent(node, tree):
    
    # if the root is a noun or a past participle, the agent is likely a by-agent
    if tree.root_pos.startswith("N") or tree.root_pos == "VBN":
        if previous_node(node, tree):
            if previous_node(node,tree).lemma == "by" and node.pos.startswith("N"):
                if next_node(node, tree):
                    if next_node(node, tree).pos == "NNP": # check if it's multi-word
                        return node.word + " " + next_node(node, tree).word
                else:
                    return node.word

    # if the root is an active verb
    elif tree.root_pos.startswith("V") and tree.root_lemma != "be":
        if parent(node, tree):
            if parent(node, tree).node_id == tree.root_id and node.type == "SBJ" and node.pos == "NNP":
                agent = node.word
                if previous_node(node, tree):
                    if previous_node(node, tree).pos == "NNP":
                        agent = previous_node(node, tree).word + " " + agent
                if next_node(node, tree):
                    if next_node(node, tree).pos == "NNP":
                        agent = agent + " " + next_node(node, tree).word
                return agent
            
def get_animate_object(node, tree):
    
    # basically just check for OBJ and pos NNP
    if node.type == "OBJ" and node.pos == "NNP":
        obj = node.word
        if previous_node(node, tree):
            if previous_node(node, tree).pos == "NNP":
                obj = previous_node(node, tree).word + " " + obj
        if next_node(node, tree):
            if next_node(node, tree).pos == "NNP":
                obj = obj + " " + next_node(node, tree).word
        return obj
    
def get_inanimate_object(node, tree):
    
    # basically just check for OBJ and pos NN
    if node.type == "OBJ" and node.pos == "NN":
        obj = node.word
        if previous_node(node, tree):
            if previous_node(node, tree).pos == "NN":
                obj = previous_node(node, tree).word + " " + obj
        if next_node(node, tree):
            if next_node(node, tree).pos == "NN":
                obj = obj + " " + next_node(node, tree).word
        return obj
        
def find_arguments(ticker, verbose):
    if verbose: print "Identifying arguments..."
    
    events = []
    
    for tree in ticker:
        # will use tree.lexical unit
        if True: #tree.lexical_unit != None:
            event = Event()
            event.ticker = tree.ticker
            event.minute = tree.minute
            event.event_id = tree.tree_id
            event.frame = "" #tree.lexical_unit
            event.tree = tree
            event.text = ""
            # get generic agents and objects
            for node in tree.nodes:
                event.text = event.text + node.word + " " # get just text
                if get_agent(node, tree) != None:
                    event.agent = get_agent(node, tree)
                if get_animate_object(node, tree) != None:
                    event.animate_obj = get_animate_object(node, tree)
                if get_inanimate_object(node, tree) != None:
                    event.inanimate_obj = get_animate_object(node, tree)
                #means = get_means(node, tree)
        
                #if agent != None: print "agent is", agent
                #if obj != None: print "object is", obj
            
            # get specific arguments for different events
            # test first with substitutions
            if tree.root_lemma == "replace": event.frame = "Substitute"
            if tree.root_lemma == "assist": event.frame = "Pass"
            if tree.root_lemma == "pass": event.frame = "Pass"
            if tree.root_lemma == "foul": event.frame = "Foul"
            
            if event.frame == "Substitute":
                if event.agent != None: event.arguments["SUBSTITUTE"] = event.agent
                if event.animate_obj != None: event.arguments["SUBSTITUTED_PLAYER"] = event.animate_obj
            
            if event.frame == "Pass":
                if event.agent != None: event.arguments["PASSER"] = event.agent
                recipient = re.search("to\s([A-Z][a-z]+(\s[A-Z][a-z]+)?)", event.text) # or look for inanimate indirect object generally?
                if recipient: event.arguments["RECIPIENT"] = recipient.group(1)
                
            if event.frame == "Foul":
                if event.agent != None: event.arguments["OFFENDER"] = event.agent
                if event.animate_obj != None: event.arguments["OFFENDED_PLAYER"] = event.animate_obj
            
            events.append(event)
    # return a list of events with arguments identified
    return events

##################################################

def to_asp(events, write=True):
    # TODO: import metadata lists ...
    # Question: What values do we want as integers, and not strings???
        # Minute? 
        # Ticker_ID? 
        # Event_ID?
    team_one = "arsenal"
    team_two = "hull city"
    playerdata_one = [('A','Red'),('B','Red'),('C','Red'),('D','Red'),('E','Red'),('F','Red'),('G','Red'),('H','Red'),('I','Red'),('J','Red'),('K','Red')]
    benchdata_one = [('L','Red'),('M','Red'),('N','Red'),('O','Red'),('P','Red'),('Q','Red'),('R','Red'),('S','Red'),('T','Red')]
    playerdata_two = [('AA','Blue'),('BB','Blue'),('CC','Blue'),('DD','Blue'),('EE','Blue'),('FF','Blue'),('GG','Blue'),('HH','Blue'),('II','Blue'),('JJ','Blue'),('KK','Blue')]
    benchdata_two = [('LL','Blue'),('MM','Blue'),('NN','Blue'),('OO','Blue'),('PP','Blue'),('QQ','Blue'),('RR','Blue'),('SS','Blue'),('TT','Blue')]
    
    asp = []

    ## META SECTION ##
    # Match Statistics Initialization.
    asp.extend((    '#const maxScore=20.',
                    'score(0..maxScore).',
                    'time(0).'
                    ))

    # Teams --> FROM METADATA:
    asp.extend((    'team(%s, 1).' % team_one,
                    'team(%s, 2).' % team_two
                    ))

    # Players & Their Teams: Team One --> FROM METADATA
    for p,t in playerdata_one:
        asp.append( 'player(%s,%s).' % (p,t) )

    for b,t in benchdata_one:
        asp.append( 'bench(%s,%s).' % (b,t) )

    # Players & Their Teams: Team Two --> FROM METADATA
    for p,t in playerdata_two:
        asp.append( 'player(%s,%s).' % (p,t) ) 

    for b,t in benchdata_two:
        asp.append( 'bench(%s,%s).' % (b,t) )

    ## FOR EASE OF USE DOWN THE ROAD... ##
    asp.extend((    'player(P) :- player(P,T).',
                    'team(T)   :- player(P,T).',
                    'bench(P)  :- bench(P,T).'
                    ))

    ## FLUENTS ##
    asp.extend((    'fluent(score(S,T)) :- score(S), team(T).',
                    'fluent(ball(P))    :- player(P).',
                    'fluent(player(P))  :- player(P).',
                    'fluent(player(P))  :- bench(P).',
                    'fluent(bench(P))   :- player(P).',
                    'fluent(bench(P))   :- bench(P).'
                    ))

    ## INITS ##
    asp.extend((    'init(score(0,T))    :- team(T).',
                    'init(player(P))     :- player(P).',
                    'init(bench(P))      :- bench(P).',
                    'init(neg(ball(P)))  :- player(P).'
                    ))

    ###########################

    ## DYNAMIC ACTION!!!
    for i in events:
        a = i.ticker
        b = i.minute
        c = i.text.split()
        d = i.arguments
        e = i.event_id
        f = i.frame
        asp.append( 'ticker(%s,%s,%s,%s).' % (e,b,f.lower(),a) )

        for j in i.arguments:
            asp.append( 'attribute(%s,%s,%s,%s,%s).' % (e,b,f.lower(),j.lower(),i.arguments[j].lower()) )

    if write == True:
        f = open('../game_instance.lp','w+')
        for item in asp:
            f.write(item + '\n')
        f.close()

    return asp

##################################################

# for testing
def main():
    import optparse
    parser = optparse.OptionParser()
    parser.add_option("--kicktionary", dest="kicktionary", help="location of kicktionary xml file", default="../../data/kicktionary.xml")
    parser.add_option("--ticker", dest="ticker", help="location of parsed ticker conll file", default="../../data/p4.parsed")
    parser.add_option("--verbose", dest="verbose", help="print helpful messages about the progress", default=False)
    parser.add_option("--language", dest="language", help="language of tickers (en or de)", default="en")
    ## will need to add verbnet XML file(s) at some point, maybe a complete folder
    parser.add_option("--verbnet", dest="verbnet", help="location of folder with verbnet xml files", default="../../data/verbnet")
    parser.add_option("--luorder", dest="luorder", help="location of folder with lexical unit order file", default="../../data/luorder")
    (options, args) = parser.parse_args()

    verbose = options.verbose
    language = options.language
        
#    kicktionary = read_kicktionary(options.kicktionary, verbose, language)
    ticker = read_ticker(options.ticker, verbose, language)
#    verbnet = read_verbnet(options.verbnet)

 #   luorder = [line.rstrip('\n') for line in open(options.luorder).readlines()]
 #   ticker_with_lus = kicktionary_lookup_possible_lu(kicktionary, ticker, verbose, luorder)
    
    events = find_arguments(ticker, verbose)

##################################################
    ## TESTING THE ASP CONVERSION FUNCTIONALITY ##
    asp = to_asp(events)
    for i in asp:
        print i
##################################################
    
 #   for event in events:
 #       if event.frame != None and event.frame != "":
 #           print event.ticker
 #           print event.minute
 #           print event.text
 #           print event.arguments
 #           print ""

if __name__ == "__main__":
    main()