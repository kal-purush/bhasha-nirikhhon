#!/usr/bin/env python
 
""" This file defines the graph object """

class Graph ( object ):
    """ 
    The Graph object represents the structure where the Controller will move robots 
    """

    def __init__ ( self, sector ):
        """ 
        Creates the Graph object according to the specifications given in sector
        
        Arguments :
        - sector   -- a json structure representing the sector
        """
        
        # Create the sector
        self.id =       sector['id']
        self.length =   sector['length']
        self.next_ids = sector['next']

    