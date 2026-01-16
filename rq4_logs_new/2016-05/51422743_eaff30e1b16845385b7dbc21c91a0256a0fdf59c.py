# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 17:28:11 2015

@author: Brendon

This is a script which is used for generating experimental switch sequences
"""

import numpy as np
import numpy.matlib as ml
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd
import csv
import sys
import copy

def in_to_out_pattern(List):
	NewList=[]
	for i in xrange(len(List)):
		if i % 2 == 1:
			new = List[-1]
			List.pop(-1)
		else:
			new = List[0]
			List.pop(0)
		NewList = NewList + [new]
	return NewList


class Counter(object):
	"""
	this is a simple counter object that consists
	of an array of integers which can have a maximum length
	"""
	def __init__(self, lengths):
		if not(isinstance(lengths , (list, tuple)) and
			   all([isinstance(l, int) for l in lengths])):
			raise IOError('input "lengths" must be a list of ints')
		self.lengths = lengths
		self.counter = [0] * len(lengths)

	def __len__(self):
		return len(self.counter)

	def increment(self):
		for k in range(len(self)):
			if self.counter[k] == (self.lengths[k] - 1):
				self.counter[k] = 0
			else:
				self.counter[k] += 1
				break
		return self.counter

	def __getitem__(self, i):
		return self.counter[i]

	def __call__(self):
		return self.counter
		

class Switch(object):
	""" This class encapsulates information about a given experimental switch. 
	Switches have the following properties:
	
	Attributes:
	
	Obligatory Inputs:
	name=this is text field to give a name to the switch to carry throughout.
	
	states=this is a list (of objects. The objects can be integers, strings, lists,
						   for superblock switches, these will be lists of Switch objects
						   which must have the same name as this switch)
	  
	Optional Inputs:					 
	timescale=this is an integer (default 0)- this indicates the time slot alloted for this switch.
			  Later, when we form an experiment sequence, for truely unique experiment states,
			  each timescale must be unique. Smaller integers for timescale correspond to faster
			  switches. If this switch does not need to be unique, then it can have the same
			  timescale as another switch.
	
	force_unique=this is a boolean value (normally True). Later, when we for the experiment sequence, we will
				 will want unique experiment states with respect to certain experiment paramters.
				 Setting this to true will include this as one of those parameters.
				 
	degeneracy=this is an integer (normally 1). This indicates how many times to repeat the switch
				sequence before moving onto the next switch timescale.
				
	configuration=is a specially formatted list with two inputs. The first input is one of the
				  following strings: 'sequence','random permutation', or 'random sequence'.
				 -If the first input is 'sequence': the second input should be a list
					  of indices corresponding to states that should be called. These 
					  states will be called in the sequence provided. 
				 -If the first input is 'random permuation': the second input should also be
					 a list of indices. The states that are called will be a random permutation
					 of these indices.
				 -If the first input is 'random sequence': the second input should be list of
					 lists. When the sequence is called, it will produce a random sequence,
					 and follow it deterministically.
					 
				 There are some shortcuts. If you insert:
				 configuration='sequence', it will assume that you mean configuration=['sequence',xrange(0,len(states))]
				 configuration='random permutation', it will assume	 configuration=['random permutation',xrange(0,len(states))]
				 
				 if you have a binary switch, there are some additional options included
				 for convenience,
				 configuration='AB' is equivalent to 'sequence', configuration=['sequence',[0,1]]
				 configuration='rand(A,B)' is equivalent to 'random permutation', configuration=['random permutation',[0,1]]
				 configuration='ABBA' is equivalent to configuration=['sequence',[0,1,1,0]]
				 configuration='rand(AB,BA)' is equivalent to configuration=['random sequence',[[0,1],[1,0]]]
				 configuration='rand(ABBA,BAAB)' is equivalent to configuration=['random sequence',[[0,1,1,0],[1,0,0,1]]
				 
				 for convenience, there is also a:
				 configuration='ascend/descend' option which is equivalent to configuration=['serial sequence',[[0,...,N],[N,...,0]]]
				
	switch_description=this is an optional text field that can describe the switch, and the
						experiment parameters that are dependent on this switch state.
	"""
	def __init__(self,
				 name,
				 states,
				 timescale=0,
				 force_unique=True,
				 degeneracy=1,
				 configuration='sequence',
				 switch_description='',
				 correlated_switches=[]):
		#lets do some type checking on the inputs.
				 
		if not(isinstance(name,str)):
			raise RuntimeError('name must be a string')  
		if not(isinstance(states,list)):
			raise RuntimeError('states must be a list') 
			#i might want to do additional checking here to ensure that states
			#doesn't go too crazy regarding what is in the list, and check the
			#names is the objects are of the Switch class.
		if not(isinstance(timescale,int) and timescale>=0):
			raise RuntimeError('timescale must be an integer i>=0')
		if not(isinstance(force_unique,bool)):
			raise RuntimeError('force_unique must be Boolean')
		if not(isinstance(degeneracy,int) and degeneracy>=0):
			raise RuntimeError('degeneracy must be a positive integer')
		if isinstance(configuration,list) or isinstance(configuration,str):
			if isinstance(configuration,str):
				StringConversion={'sequence':['sequence',range(0,len(states))],
								  'random permutation':['random permutation',range(0,len(states))],
								  'AB':['sequence',[0,1]],
								  'BA':['sequence',[1,0]],
								  'rand(A,B)':['random permutation',[0,1]],
								  'ABBA':['sequence',[0,1,1,0]],
								  'BAAB':['sequence',[1,0,0,1]],
								  'rand(ABBA,BAAB)':['random sequence',[[0,1,1,0],[1,0,0,1]]],
								  'ascend/descend':['serial sequence',[range(0,len(states)),range(len(states)-1,-1,-1)]],
								  'in to out':['sequence',(in_to_out_pattern(range(0,len(states)))[::-1])],
								  'in to out and back':['serial sequence',[(in_to_out_pattern(range(0,len(states)))[::-1]),(in_to_out_pattern(range(0,len(states))))]]}
				if not(configuration in StringConversion.keys()):
					raise RuntimeError('the configuration string shortcut that you have attempted to use is not valid.')
				else:
					configuration=StringConversion[configuration]
			else:
				if len(configuration)!=2:
					raise RuntimeError('the configuration string is not structured properly - it must be a list of length 2')
				else:
					AllowedStrings=['sequence','random permutation','serial sequence','random sequence']
					if configuration[0] in AllowedStrings:
						if isinstance(configuration[1],list):
							if configuration[0] in ['sequence','random permutation']:
								if any([not(0<=s<len(states)) for s in configuration[1]]):
									raise RuntimeError('the indices in the configuration list are out of bounds')
							else:
								if any([any([not(0<=s<len(states)) for s in List]) for List in configuration[1]]):
									raise RuntimeError('there is a problem with the configuration of the configuration sequence list')
						else:
							raise RuntimeError('the second entry in the configuration list must be a list')
					else:
						raise RuntimeError('the first entry in the configuration list must be one of the following '+str(AllowedStrings))
		else:
			raise RuntimeError('configuration must be a properly formatted string or list with two entries')
		if not(isinstance(switch_description,str)):
			raise RuntimeError('switch_description must be a string')
		if not(isinstance(correlated_switches,list)):
			raise RuntimeError('correlated switches must be a string')
			  
		self.name=name
		self.states=states
		self.timescale=timescale
		self.force_unique=force_unique
		self.degeneracy=degeneracy
		self.configuration=configuration
		self.switch_description=switch_description
		self.correlated_switches=correlated_switches
		self.index=-1 #this is an index for propagating through a list - this is not for entry

	def __len__(self):
		return len(self.states)

	def __str__(self):
		output = (['\t'.join([self.name, 
							  'timescale: ' + str(self.timescale),
							  'configuration: ' + str(self.configuration),
							  'unique: ' + str(self.force_unique)])] 
				  + ['\t' + item for state in self.states
								for item in str(state).split('\n')])
		return '\n'.join(output)

	def __repr__(self):
		return str(self) 
		
	def correlate_with(self,CorrelatedSwitch):
		"""This function changes the 'self' state such that it conforms to the input Correlated switch such
		that the two are correlated, and notes that this state should be dependent on the other state."""
		if CorrelatedSwitch.__class__.__name__!='Switch':
			raise RuntimeError('the input to the function must be a switch object that you would like to correlate with.')
		if len(self.states)!=len(CorrelatedSwitch.states):
			raise RuntimeError('the two states must have the same number of states in order for them to be correlated with each other')
		#since this Switch is now dependent on the CorrelatedSwitch, it is no longer possible to ensure uniqueness (in fact it is contradictory)
		self.force_unique=False
		self.degeneracy=CorrelatedSwitch.degeneracy
		self.timescale=CorrelatedSwitch.timescale
		self.configuration=CorrelatedSwitch.configuration
		if not(CorrelatedSwitch.name in self.correlated_switches):
			self.correlated_switches+=[CorrelatedSwitch.name]
		return self
		
	def generate_sequence(self,length=1,index=-1,states=False):
		"""This function uses the prescribed rules in self to generate a sample sequence for this switch
		   The input 'length' specifies how many sequences to generate in a row.
		   In the case of the serial sequence, I might want to use a starting index, 
		   specified by the 'index' input, or if states=True, then specified by the state"""
		Sequence=[]
		method=self.configuration[0]
		inds=self.configuration[1]
		for i in xrange(0,self.degeneracy*length):
			if method=='sequence':
				seq=inds
			elif method=='serial sequence':
				#propagate forward the index property
				if index<0:
					self.index=(self.index+1)%len(inds)
					index=self.index
				seq=inds[self.index]
			elif method=='random sequence':
				seq=inds[np.random.random_integers(0,len(inds))-1]
			elif method=='random permutation':
				perm=list(np.random.permutation(len(inds)))
				seq=[inds[s] for s in perm]
			Sequence+=seq
		if states:
			Sequence = [self.states[s] for s in Sequence]
		return Sequence
		
	def nest(self):
		#nest a switch without much worry
		return Switch(self.name,[self],force_unique=False,timescale=0)
	   

class Sequence(object):
	"""
	This is an object which consists of an array of switches which defines
	rules for creating experimental switch sequences. This object is provided
	with a list of Switch objects, produces sequences, and plots sequences.
	"""
	def __init__(self, switches):
		if not(isinstance(switches, list) and
			   all([isinstance(s, Switch) for s in switches])):
			raise IOError('input must be a list of Switch objects')
		self.switches = switches
		self.current_sequence = None
		self.generate_nested_switch_sequence()
		self.current_figure = None

	def __len__(self):
		return len(switches)

	def __str__(self):
		return 'Sequence:\n' + '\n'.join([str(s) for s in self.switches])

	def generate_switch_sequence(self, states=False):
		"""
		This function takes in an array of switch objects and attempts to generate a 
		switch sequence consistent with the rules included for each switch.

		- first we will need to take a look at the relative timescales of the switches
		  and whether or not we need to force uniqueness
		
		- if there is more than one switch that requires uniqueness in a given timeslot,
		  the configuration rules will be compromised in favor of randomness
		
		- if there is more than one switch in a given time slot, and only one of them
		  is forced unique, those that are not forced unique may not even exhibit full
		  sequences.   
		
		- compromises will be necessary to satisfy the input rules, but we should explicitly 
		  state the compromises made as warnings to the user.
		"""
		
		# any switch with only one state already meets the uniqueness criterion,
		# so I need not force uniqueness:
		for i in xrange(len(self.switches)):
			if len(self.switches[i].states) == 1:
				self.switches[i].force_unique = False
		
		# get a list of the timescales
		timescales  = [switch.timescale	for switch in self.switches]
		forceunique = [switch.force_unique for switch in self.switches]

		# find the unique timescales, and sort from slowest to fastest.
		unique_timescales = sorted(list(set(timescales)), reverse=True)
	   
		# if there aren't any force contraints on a timescale, choose one to be unique
		# (don't elliminate the timescale use because there isn't a force unique contraint)
		forceunique = check_force_unique_across_timescales(timescales,
														   unique_timescales,
														   forceunique)
		
		sequences	= [[]]*len(self.switches)
		multiplicity = 1

		for i in xrange(0,len(unique_timescales)):

			# find all of the switch indices for this timescale
			Inds = [s for s in xrange(0,len(timescales)) if timescales[s]==unique_timescales[i]]

			# see if any of the switches are conflicting in attempting uniqueness
			force_unique = [forceunique[s] for s in Inds]
			
			# deal with the parameters that require uniqueness first:
				
			# the principle here is that I simply generate a random sequence of the
			# unique sets of each set - i can't really do this without randomness
				
			# here are the switch indices for which we need to solve this problem.
			forceInds = [Inds[i] for i in xrange(0,len(force_unique)) if force_unique[i]==True]
				
			counter = 0
			for j in xrange(0, multiplicity):
				if len(forceInds)>1:
					sequence = [self.switches[ink].generate_sequence(index=j, states=states) for ink in forceInds]
					#this function sorts out how to deal with this problem while introducing randomness
					sequence = same_timescale_cooperation(sequence)
		
					counter += len(sequence[0])
					for i in xrange(0, len(forceInds)):
						ink = forceInds[i]
						sequences[ink] = sequences[ink] + [sequence[i]]
				
				else:
					ink = forceInds[0]
					sequence = self.switches[ink].generate_sequence(states=states)#(index=j)
					counter += len(sequence)
					sequences[ink] = sequences[ink] + [sequence]
						
			ink = forceInds[0]
				
			#now deal with the force_unique=False crowd:
			ills = [Inds[ind] for ind in xrange(0,len(force_unique)) 
									  if force_unique[ind] == False]
			for ill in ills:
				index = 0
				for subsequence in sequences[ink]:
					false_subsequence = []
					while True:
						false_subsequence = false_subsequence + self.switches[ill].generate_sequence(states=states)
						index += 1
						if len(false_subsequence)>=len(subsequence):
							break
					# trim the subsequence if it is too long, and then add it to the sequence array
					false_subsequence = false_subsequence[0:len(subsequence)]
					sequences[ill] = sequences[ill] + [false_subsequence]
				
			#now deal with the parameters that do not require uniqueness
			multiplicity = counter
		
		#currently sequences is organized in a nested structure, expand it: 
		return sequence_structure_to_sequence(timescales, sequences)

	def generate_nested_switch_sequence(self):
		""" this function is similar to the 'generate_state_sequence' function except that
		this one is more general in that it checks for nested block structures and
		solves for a block sequence."""
		
		#generate the first round sequence:
		sequence_states = self.generate_switch_sequence(states=True)
		#generate the states array since the index array might be deceiving:
		#sequence_states = ChangeBasisInSequence(sequence, switches=self.switches)
		
		#I would also like to count states on each nesting level so that I have
		#a state index, a block index, a superblock index, and uberblock index, ...
		state_index = [range(0,len(sequence_states[0]))] # start out just with the state indices.	
		
		#iterate through the state sequence list and expand until no nesting remains:
		nesting_count = 1
		while nesting_count > 0:

			sequence_length = len(sequence_states[0])
			nesting_count = 0
			#now for each state in the state sequences, I will expand the sequence
			#if there is nesting:
			state_index=state_index+[[]]
			
			insertindex=0
			for i in xrange(0,sequence_length):
				state_is_switch = [isinstance(sequence_states[ind][insertindex], Switch) 
											  for ind in xrange(0,len(sequence_states))]
				nesting = any(state_is_switch)

				if nesting:
					nesting_count += 1
					#then, I need to make a new switch list:
					new_switches = []
					for j in xrange(len(self.switches)):
						newstate = sequence_states[j][insertindex]
						name = self.switches[j].name
						if isinstance(newstate, Switch):
							newswitch = newstate
							newswitch.name = name
						else:
							newswitch = Switch(name,[newstate],force_unique=False)
						new_switches = new_switches + [newswitch]
					#now that I have a new switch array, I can generate a new sequence:
					new_sequence_states = Sequence(new_switches).generate_switch_sequence(states=True)

					#but make sure that I switch from indices to states:
					#new_sequence_states = ChangeBasisInSequence(new_sequence, switches=new_switches)
					
					#now insert this state sequence into the original sequence:
					for j in xrange(0,len(self.switches)):
						sequence_states[j].pop(insertindex)
						sequence_states[j][insertindex:insertindex] = new_sequence_states[j]
					#now adjust the state indices:
					state_index[len(state_index)-1] = state_index[len(state_index)-1] + range(0,len(new_sequence_states[0]))
					#now i need to expand the previous state indices
					for j in xrange(0,len(state_index)-1):
						Ind = state_index[j].pop(insertindex)
						state_index[j][insertindex:insertindex] = [Ind]*len(new_sequence_states[0])
						
					insertindex+=len(new_sequence_states[0])
				else:
					state_index[len(state_index)-1]=state_index[len(state_index)-1]+[0]
					insertindex+=1

					
		zeros = state_index.pop(-1)
		state_index.reverse()
		state_index=state_index+[zeros]
						
		#although the problem is already solved now, perhaps in some cases I would
		#prefer a states list and index sequence rather than the state sequence
		#(for example for plotting)
		
		#First generate the states list:
		states=[sorted(list(set(statesequence))) for statesequence in sequence_states]
		#now iterate through the state sequence list and find the appropriate index.
		sequence_index=[[]]*len(self.switches)
		for i in xrange(0,len(self.switches)):
			sequence_index[i]=[states[i].index(state) for state in sequence_states[i]]
		names = [switch.name for switch in self.switches]
			
		self.current_sequence = SequenceInstance(names, states, sequence_index, sequence_states, state_index)
		return self.current_sequence

	def __call__(self):
		return self.generate_nested_switch_sequence()

	def plot_switch_pattern(self, linewidth=2,width=1,states=-1,axis=-1,time=-1,xlabel='state index',add_blocks=False):
		"""This function plots the digital switch pattern for each switch.
		It might we worthwhile later to construct 'block' plots that include deadtime."""

		# if not(states<0):
		#	 #in this case, I am inserting states lists
		#	 for i in range(len(self)):
		#		 Switches[i].states=states[i]
			
		# order the switches by timescale, and within timescale by importance
		timescales=[switch.timescale for switch in self.switches]
		forceunique=[switch.force_unique for switch in self.switches]
		# find the unique timescales, and sort from slowest to fastest.
		unique_timescales=sorted(list(set(timescales)),reverse=True)
		Indices=[]
		for un in unique_timescales:
			Inds=[ind for ind in xrange(0,len(timescales)) if timescales[ind]==un]
			force_unique=[forceunique[s] for s in Inds]
			if sum(force_unique)>0:
				Inks=[ink for ink in xrange(0,len(Inds)) if force_unique[ink]==True]
				for ink in Inks:
					Inds=[Inds.pop(ink)]+Inds
			Indices+=Inds
		switches  = [self.switches[ind] for ind in Indices]
		sequences = [self.current_sequence[ind] for ind in Indices]
		
		if axis<0:
			fig = plt.figure(figsize=(6*width,1*len(switches)))
			ax = fig.add_subplot(111)
		else:
			ax=axis
		
		condense=.7 # this sets the relative spacing between the plots
		PlotNumber=len(sequences)
		if add_blocks==True:
			PlotNumber+=1
		for i in xrange(0,PlotNumber):
			if time<0:
				t=np.array(range(len(sequences[i])))+.5
			else:
				t=list(np.array(time)+.5)
			t=list(t)+[max(t)+1]
			block_width=60*.9/(60+len(sequences[0]))
			if add_blocks==True and i==PlotNumber-1:
				for j in xrange(0,len(sequences[0])):
					ax.add_patch(patches.Rectangle((j+1-block_width/2,i-condense/2), block_width,condense,hatch='\\',facecolor='k',edgecolor=None))
			else:
				y=np.array(sequences[i])
				y=y-min(y)
				if len(set(y))>1:
					y=condense*(y/float(max(y))-.5)+i
				else:
					y=y+i
				y=list(y)+[y[-1]]
				line=ax.step(t,y, '-', linewidth = linewidth, where='post')#,label=Switch[i].name)
				color=line[0].get_color()
				
				ax.text(max(t)+(float(max(t)-min(t))/30)*(1+1),i,switches[i].name,color=color,verticalalignment='center',horizontalalignment='left',fontsize=15,fontname='Corbel')
				Len=len(switches[i].states)
				if Len<6:
					for j in xrange(0,Len):
						state=switches[i].states[j]
						if state>=0 and not(isinstance(state,str)):
							state='+'+str(state)
						if Len!=1:
							ax.text(max(t)+(float(max(t)-min(t))/40)*(.5),condense*(j/float(Len-1)-.5)+i,str(state),color=color,verticalalignment='center',fontsize=10,fontname='Corbel')
						else:
							ax.text(max(t)+(float(max(t)-min(t))/40)*(.5),i,str(state),color=color,verticalalignment='center',fontsize=12,fontname='Corbel')
			 
		ax.set_xlim([min(t),max(t)])
		ax.set_ylim([-.5, PlotNumber-.5])
		ax.set_xlabel(xlabel) 
		ax.spines['top'].set_visible(False)
		ax.spines['left'].set_visible(False)
		ax.spines['right'].set_visible(False)
		ax.get_xaxis().tick_bottom()
		ax.get_yaxis().tick_left()
		plt.gca().yaxis.set_major_locator(plt.NullLocator())
		#ax.get_yaxis().tick_right()
		
		#here is some info for drawing nested diagrams:
		nest=[min(t),max(t),1-block_width/2,1+block_width/2,PlotNumber-1+condense/2]
		if axis<0:
			plt.show()
		else: 
			return ax,nest

	def plot(self, block=None, width=1, linewidth=1, save=-1):
		""" The purpose of this function is to make a series of plots showing
			the nested switch sequence structures in an experimental switch sequence.
			
			The 'block' input is an array corresponding to the particular blocks
			that you want to plot on each level. By default, it will just show
			the first block on each level.
		"""
		#these are the names for the x axis.	 
		nested_names=['state','block','superblock','uberblock','duberblock',
					  'tuberblock','quberblock','quiberblock','hexberblock',
					  'septerblock','octerblock']

		state_index  = self.current_sequence.state_index
		names        = self.current_sequence.names
		sequences    = self.current_sequence.index_sequence
		states       = self.current_sequence.states

		if block is None:
			block = [0] * (len(state_index)-1)

		if isinstance(block, int):
			block = [block]

		if len(block) < (len(state_index) - 1):
			block = block + [0] * (len(state_index) - 1 - len(block))

		ax = []
		#if save<0:
		enlargement = 1
		#else:
		#	Enlargement=1000
		nested_levels = len(state_index)-1
		nest = [[]] * nested_levels

		# create the figure with an appropriate size
		fig = plt.figure(figsize=(enlargement * 6 * width,
								  enlargement * nested_levels * .35 * len(names)))
		fig.subplots_adjust(hspace=.5)

		used_switches = []
		for k in xrange(nested_levels):	
		# we will need to figure out which switches take place on which timescale,
		# but we need to keep in mind that switches can change timescale from block
		# to block and superblock to superblock.

		# the number of timescales are set by StateIndex
		# first get the subsequence that corresponds to the block that I will plot
		# on the lowest timescale, then determine which switches change in that block
		# and choose those for plotting.
			level = k + 1
			# here are the indices for the first plot:
			# find the indices corresponding to what we need
			if k == 0:
				state_index = np.array(state_index)

			In0 = np.transpose(ml.repmat(np.array(block[level-1:]),
										 len(state_index[level]),1))
			In1 = sum(In0 == state_index[level:,:])
			In  = (In1==len(block[level-1:]))
			
			Inds = [ind for ind in xrange(len(In)) if In[ind]]

			#now obtain the subsequence:
			Subsequence = [[]] * len(sequences)
			
			ChangeSwitches = []

			for i in xrange(0,len(sequences)):
				subsequence = [sequences[i][inds] for inds in Inds]
				Subsequence[i] = subsequence
				
				if len(set(subsequence))>1 and not(i in used_switches):
					ChangeSwitches = ChangeSwitches+[i]
			#so now I have the subsequence, and I have the list of switch indices to use
			#now I just need to package this for plotting.
			used_switches = used_switches + ChangeSwitches

			NewSwitches=[]
			NewSequences=[]
			NewIndex=[state_index[level-1][ind] for ind in Inds]
			for switch in ChangeSwitches:
				newswitch = copy.deepcopy(self.switches[switch])
				newswitch.states = states[switch]
				NewSwitches  = NewSwitches  + [newswitch]
				newsequence  = Subsequence[switch]
				NewSequences = NewSequences + [newsequence]
				
			#reduce the sequences so that they are simpler on the next level:
			UNewSequences=[]
			UNewIndex=list(set(NewIndex))
			for sequence in NewSequences:
				usequence=[sequence[NewIndex.index(newind)] for newind in UNewIndex]
				UNewSequences=UNewSequences+[usequence]

			ax.append(fig.add_subplot(100*nested_levels+11+k))

			#make a figure:
			NewSwitches.reverse()
			NewSequences.reverse()
			UNewSequences.reverse()
			ax[k],nest[k] = plot_switch_pattern(NewSwitches,UNewSequences,
													linewidth=linewidth,width=width,
													axis=ax[k],time=UNewIndex,
													xlabel=nested_names[level-1],
													add_blocks=True)

		#now that we have contructed the plot, we want to connect lines between subplots
		#that indicate a nesting structure

		lines=[]
		for k in xrange(len(nest)-1):
			transFigure = fig.transFigure.inverted()

			coord1 = transFigure.transform(ax[k].transData.transform([nest[k][0],-.5]))
			coord2 = transFigure.transform(ax[k+1].transData.transform([nest[k+1][2]+block[k],nest[k+1][4]]))
			coord3 = transFigure.transform(ax[k].transData.transform([nest[k][1],-.5]))
			coord4 = transFigure.transform(ax[k+1].transData.transform([nest[k+1][3]+block[k],nest[k+1][4]]))

			line1 = matplotlib.lines.Line2D((coord1[0],coord2[0]),(coord1[1],coord2[1]),
											   transform=fig.transFigure,color='k',alpha=.3) 
			line2 = matplotlib.lines.Line2D((coord3[0],coord4[0]),(coord3[1],coord4[1]),
											   transform=fig.transFigure,color='k',alpha=.3)
			lines=lines+[line1,line2]									   

		fig.lines = lines
		self.current_figure = fig
		return

class SequenceInstance(object):
	"""
	this is a given instance of a sequence generated by a Sequence object
	"""

	nested_names = ['trace','state','block','superblock','uberblock',
				   'duberblock','tuberblock','quberblock','quiberblock','hexberblock',
				   'septerblock','octerblock']

	def __init__(self, names, states, index_sequence, state_sequence, state_index):
		self.names	= names
		self.states   = states
		self.index_sequence = index_sequence
		self.state_sequence = state_sequence
		self.state_index	= state_index
		self.table = self.to_dataframe()

	def to_dataframe(self):
		index = [range(len(self.state_index[0]))] + self.state_index[:-1]
		index_names = [self.nested_names[i % len(self.nested_names)]
		               for i in range(len(index))]
		df = pd.DataFrame({
	            k: v for k, v in zip(self.names, self.state_sequence)
	        }, index=index)
		df.index.names = index_names
		return df

	def __repr__(self):
		return self.table.__repr__()

	def _repr_html_(self):
		return self.table._repr_html_()

	def save(self, filename, sep='\t', **kwargs):
		"""
		saves the experimental sequence to a csv file
		with tab delimiters by default.
		"""
		self.table.to_csv(filename, sep=sep, **kwargs)
		return


def sequence_structure_to_sequence(timescales, sequences):
	"""
	take in a sequence structure "sequences" and generate an actual extended sequence "Extended".	
	"""
	
	#reverse the timescale ordering now,
	unique_timescales=sorted(list(set(timescales)))
	#now repeat states as necessary to build the sequence
			
	Extended=[[]]*len(sequences)
	for i in xrange(0,len(unique_timescales)):
		Inds=[s for s in xrange(0,len(timescales)) if timescales[s]==unique_timescales[i]]
#		force_unique=[forceunique[s] for s in Inds]

#			ink=Inds[force_unique.index(True)]
		for j in xrange(0,len(Inds)):
			ink=Inds[j]
			sequence=sequences[ink]
			extended=[]
			lengths_new=[]
			if i==0:
				lengths=[]
			index=0
			for seq in sequence:
				counter=0
				for s in seq:
					if i==0:
						lengths+=[1]
					extended+=[s]*lengths[index]
					counter+=lengths[index]
					index+=1
				if j==len(Inds)-1:	
					lengths_new+=[counter]
			if j==len(Inds)-1:
				lengths=lengths_new
			Extended[ink]=Extended[ink]+extended
			
	return Extended

def check_force_unique_across_timescales(timescales,unique_timescales,forceunique):
	for i in xrange(0,len(unique_timescales)):
		Inds=[s for s in xrange(0,len(timescales)) if timescales[s]==unique_timescales[i]]
		force_unique=[forceunique[ind] for ind in Inds]
		if len(Inds)==1:
			forceunique[Inds[0]]=True
		else:
			if sum(force_unique)==0:
				Int=np.random.randint(0,len(force_unique))
				forceunique[Inds[Int]]=True
	return forceunique
				

def same_timescale_cooperation(sequences):
	""" 
		This function takes states that would like to coexist on the same timescale
		and makes it work. This requires a bit of randomness, but that is to be expected.
	"""
	lengths = [len(s) for s in sequences]
	n_states = np.prod(lengths)
	sequence_out = -np.ones([n_states, len(sequences)])
	counter = Counter(lengths)
	for i in xrange(0, n_states):
		for j in xrange(0, len(sequences)):
			sequence_out[i,j] = sequences[j][counter[j]]
		counter.increment()

	# apply the random permutation of states and format for output:
	sequence_out = [list(sequence_out[j,:]) for j in
					np.random.permutation(n_states)]
	sequence_out = [list(t) for t in zip(*sequence_out)]
	return sequence_out


######### Example State Definitions ##########################

#this is a usual block configuration from Gen I.
#Switches=[Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='AB'),
#		  Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=1,configuration='ABBA'),
#		  Switch('$\\tilde{\\theta}$',[-1,1],timescale=2,configuration='ABBA'),
#		  Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=3,configuration='AB')]
		  
#what if we wanted to correlate the N switch with the E switch?
#this statement overrides the instructions above:
#Switches[0].correlate_with(Switches[1])

#this would be the normal block configuration with 1 additional level of randomness:
#Switches=[Switch('switch 1',[-1,1],timescale=0,configuration='rand(A,B)'),
#		  Switch('switch 2',[-1,1],timescale=1,configuration='rand(ABBA,BAAB)'),
#		  Switch('switch 3',[-1,1],timescale=2,configuration='rand(ABBA,BAAB)'),
#		  Switch('switch 4',[-1,1],timescale=3,configuration='rand(A,B)')]

#you can also do kind of funky stuff like this:
#Switches=[Switch('switch 1',[-1,1],timescale=1,force_unique=False,configuration=['random permutation',[0,0,1]]),#repeats states in random ordering
#		  Switch('switch 2',[-1,1],timescale=1,configuration=['random sequence',[[0,1],[1,0],[0,1,1,0],[1,0,0,1]]]),#randomly chosen sequences with different length
#		  Switch('switch 3',[-1,1],timescale=2,configuration=['serial sequence',[[0,1,1,0],[1,0,0,1]]]),#sequential change in the sequence
#		  Switch('switch 4',[-1,1],timescale=2,configuration='rand(A,B)')]

#it is also possible to configure non-binary switch states with the same language.
#Switches=[Switch('switch 1',['cat','dog','elephant'],timescale=0,configuration=['random permutation',[0,1,2]]),#repeats states in random ordering
#		  Switch('switch 2',[0,.1,.2,.3,.4],timescale=1,configuration='random permutation'),#randomly chosen sequences with different length
#		  Switch('switch 3',['a','b','c'],timescale=2,configuration=['random sequence',[[0,1,1,2],[1,0,1,2]]]),#sequential change in the sequence
#		  Switch('switch 4',[-1,0,1],timescale=3)]

#we can also force multiple switches to inhabit the same set of timescales:
#this is similar to a Gen I sequence, but with the H and E field switches on
#same timescale and the \theta and B field switches on the same timescale:
#Switches=[Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='AB'),
#		  Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=0,configuration='ABBA'),
#		  Switch('$\\tilde{\\theta}$',[-1,1],timescale=1,configuration='ABBA'),
#		  Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=1,configuration='AB')]

#or we could put all of the switches on the same timescale
#Switches=[Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='AB'),
#		  Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=0,configuration='ABBA'),
#		  Switch('$\\tilde{\\theta}$',[-1,1],timescale=0,configuration='ABBA'),
#		  Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=0,configuration='AB')]

#note that the previous switch definitions are equivalent to just increasing the degeneracy,
#since the actual ABBA and AB structures are necessarily erased when forcing multiple switches
#in one timescale.		  
#Switches=[Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,degeneracy=1),
#		  Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=0,degeneracy=2),
#		  Switch('$\\tilde{\\theta}$',[-1,1],timescale=1,degeneracy=2),
#		  Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=1,degeneracy=1)]
		  

#Now lets consider superblock switches which involve nested switch structures
#in which the farthest nested switches are the fastest. We will require some
#more general code to handle this type of nested structure, but the basic idea
#is the same.

#as a first pass, ignoring the block switches, the superblock typically looks like:
#Switches=[Switch('$\\tilde{\mathcal{P}}$',[-1,1],timescale=0,configuration='rand(A,B)'),
#		  Switch('$\\tilde{\mathcal{R}}$',[-1,1],timescale=1),
#		  Switch('$\\tilde{\mathcal{L}}$',[-1,1],timescale=2),
#		  Switch('$\\tilde{\mathcal{G}}$',[-1,1],timescale=3)]

#these were considered even a step above - uberblocks (with possible parameter variation)		  
#Switches=[Switch('parameter variation',['-10x','0','+10x'],timescale=4),
#		  Switch('$|\mathcal{B}_z|$',['0B','1B','2B'],timescale=5,configuration=['sequence',[1,0,1,2]]),
#		  Switch('$|\mathcal{E}|$',['low','high'],timescale=6,degeneracy=4),
#		  Switch('$\hat{k}\cdot\hat{z}$',[-1,1],timescale=7)] 

#these were considered even a step above - uberblocks (with possible parameter variation)		  
#Switches=[Switch('$\mathcal{B}^{nr}$',[0],timescale=0),#force_unique=False),
#		  Switch('$\mathcal{E}^{nr}$',[0],timescale=0,force_unique=False),#force_unique=False),
#		  Switch('parameter variation',['-10x','0','+10x'],timescale=4),
#		  Switch('$|\mathcal{B}_z|$',['0B','1B','2B'],timescale=5,configuration=['sequence',[1,0,1,2]]),
#		  Switch('$|\mathcal{E}|$',['low','high'],timescale=6,degeneracy=4),
#		  Switch('$\hat{k}\cdot\hat{z}$',[-1,1],timescale=7)]	 


############# Create sequences and Plot Them #####################
		  
#sequences=generate_switch_sequence(Switches)
#plot_switch_pattern(Switches,sequences,linewidth=1) 

############

def plot_switch_pattern(Switches,sequences,linewidth=2,width=1,states=-1,axis=-1,time=-1,xlabel='state index',add_blocks=False):
	"""This function plots the digital switch pattern for each switch.
	It might we worthwhile later to construct 'block' plots that include deadtime."""
	
	
	if not(states<0):
		#in this case, I am inserting states lists
		for i in xrange(0,len(Switches)):
			Switches[i].states=states[i]
		
	#order the switches by timescale, and within timescale by importance
	timescales=[switch.timescale for switch in Switches]
	forceunique=[switch.force_unique for switch in Switches]
	#find the unique timescales, and sort from slowest to fastest.
	unique_timescales=sorted(list(set(timescales)),reverse=True)
	Indices=[]
	for un in unique_timescales:
		Inds=[ind for ind in xrange(0,len(timescales)) if timescales[ind]==un]
		force_unique=[forceunique[s] for s in Inds]
		if sum(force_unique)>0:
			Inks=[ink for ink in xrange(0,len(Inds)) if force_unique[ink]==True]
			for ink in Inks:
				Inds=[Inds.pop(ink)]+Inds
		Indices+=Inds
	Switches=[Switches[ind] for ind in Indices]
	sequences=[sequences[ind] for ind in Indices]
	
	if axis<0:	
		fig = plt.figure(figsize=(6*width,1*len(Switches)))
		#ax = plt.axes(frameon=False)
		ax = fig.add_subplot(111)
	else:
		ax=axis
	
	condense=.7#this sets the relative spacing between the plots
	PlotNumber=len(sequences)
	if add_blocks==True:
		PlotNumber+=1
	for i in xrange(0,PlotNumber):
		if time<0:
			t=np.array(range(len(sequences[i])))+.5
		else:
			t=list(np.array(time)+.5)
		t=list(t)+[max(t)+1]
		block_width=60*.9/(60+len(sequences[0]))
		if add_blocks==True and i==PlotNumber-1:
			for j in xrange(0,len(sequences[0])):
				ax.add_patch(patches.Rectangle((j+1-block_width/2,i-condense/2), block_width,condense,hatch='\\',facecolor='k',edgecolor=None))
		else:
			y=np.array(sequences[i])
			y=y-min(y)
			if len(set(y))>1:
				y=condense*(y/float(max(y))-.5)+i
			else:
				y=y+i
			y=list(y)+[y[-1]]
			line=ax.step(t,y, '-', linewidth = linewidth, where='post')#,label=Switch[i].name)
			color=line[0].get_color()
			
			ax.text(max(t)+(float(max(t)-min(t))/30)*(1+1),i,Switches[i].name,color=color,verticalalignment='center',horizontalalignment='left',fontsize=15,fontname='Corbel')
			Len=len(Switches[i].states)
			if Len<6:
				for j in xrange(0,Len):
					state=Switches[i].states[j]
					if state>=0 and not(isinstance(state,str)):
						state='+'+str(state)
					if Len!=1:
						ax.text(max(t)+(float(max(t)-min(t))/40)*(.5),condense*(j/float(Len-1)-.5)+i,str(state),color=color,verticalalignment='center',fontsize=10,fontname='Corbel')
					else:
						ax.text(max(t)+(float(max(t)-min(t))/40)*(.5),i,str(state),color=color,verticalalignment='center',fontsize=12,fontname='Corbel')
		 
	ax.set_xlim([min(t),max(t)])
	ax.set_ylim([-.5, PlotNumber-.5])
	ax.set_xlabel(xlabel) 
	ax.spines['top'].set_visible(False)
	ax.spines['left'].set_visible(False)
	ax.spines['right'].set_visible(False)
	ax.get_xaxis().tick_bottom()
	ax.get_yaxis().tick_left()
	plt.gca().yaxis.set_major_locator(plt.NullLocator())
	#ax.get_yaxis().tick_right()
	
	#here is some info for drawing nested diagrams:
	nest=[min(t),max(t),1-block_width/2,1+block_width/2,PlotNumber-1+condense/2]
	if axis<0:
		plt.show()
	else: 
		return ax,nest	  

# def ChangeBasisInSequence(sequences,switches=-1,states=-1):
# 	#i generated the sequence list in terms of indices referencing the states list
# 	#but now I want the states themselves:
# 	if not(switches<0) and states<0:
# 		states=[]
# 		for i in xrange(0,len(switches)):
# 			states=states+[switches[i].states]
# 	elif states<0 and switches<0:	
# 		raise RuntimeError('you must input either switches xor states')

# 	Statesequences=[]
# 	#now that I have a states list, for each sequence I can index:
# 	for i in xrange(0,len(sequences)):
# 		sequence=sequences[i]
# 		statesequence=[states[i][int(sequence[ind])] for ind in xrange(0,len(sequence))]
# 		Statesequences=Statesequences+[statesequence]
	
# 	return Statesequences
		  
# def PlotNestedSuperblockSequence(States,sequences,state_index,BlockChoice=-1,width=1,linewidth=1,save=-1):
# 	""" The purpose of this function is to make a series of plots showing
# 		the nested switch sequence structures in an experimental switch sequence.
		
# 		The 'BlockChoice' input is an array corresponding to the particular blocks
# 		that you want to plot on each level. By default, it will just show
# 		the first block on each level.
# 	"""
# 	#these are the names for the x axis.	 
# 	NestedNames=['state','block','superblock','uberblock','duberblock',
# 				 'tuberblock','quberblock','quiberblock','hexberblock',
# 				 'septerblock','octerblock']
	
# 	#Switches=[0]*len(States)
# 	#for i in xrange(len(States))
	
	
# 	if BlockChoice<0:
# 		BlockChoice=[0]*(len(state_index)-1)
	
# 	AX=[]
# 	#if save<0:
# 	Enlargement=1
# 	#else:
# 	#	Enlargement=1000
# 	NestedLevels=len(StateIndex)-1
# 	nest=[[]]*NestedLevels
# 	fig = plt.figure(figsize=(Enlargement*6*width,Enlargement*NestedLevels*.35*len(States)))
# 	fig.subplots_adjust(hspace=.5)
# 	UsedSwitches=[]
# 	for k in xrange(NestedLevels):	
# 	#we will need to figure out which switches take place on which timescale,
# 	#but we need to keep in mind that switches can change timescale from block
# 	#to block and superblock to superblock.
	
# 	#the number of timescales are set by StateIndex
# 	#first get the subsequence that corresponds to the block that I will plot
# 		#on the lowest timescale, then determine which switches change in that block
# 		#and choose those for plotting.
# 		level=k+1
# 			#here are the indices for the first plot:
# 		#find the indices corresponding to what we need
# 		if k==0:
# 			StateIndex=m.array(StateIndex)
# 		In0=m.transpose(ml.repmat(m.array(BlockChoice[level-1:]),len(StateIndex[level]),1))
# 		In1=sum(In0==StateIndex[level:,:])
# 		In=(In1==len(BlockChoice[level-1:]))
		
# 		Inds=[ind for ind in xrange(len(In)) if In[ind]]

# 			#now obtain the subsequence:
# 		Subsequence=[[]]*len(sequences)
		
# 		ChangeSwitches=[]
	
# 		for i in xrange(0,len(sequences)):
# 			subsequence=[sequences[i][inds] for inds in Inds]
# 			Subsequence[i]=subsequence
			
# 			if len(set(subsequence))>1 and not(i in UsedSwitches):
# 				ChangeSwitches=ChangeSwitches+[i]
# 		#so now I have the subsequence, and I have the list of switch indices to use
# 		#now I just need to package this for plotting.
# 		UsedSwitches=UsedSwitches+ChangeSwitches
			 
# 		NewSwitches=[]
# 		Newsequences=[]
# 		NewIndex=[StateIndex[level-1][ind] for ind in Inds]
# 		for switch in ChangeSwitches:
# 			newswitch=Switches[switch]
# 			newswitch.states=States[switch]
# 			NewSwitches=NewSwitches+[newswitch]
# 			newsequence=Subsequence[switch]
# 			Newsequences=Newsequences+[newsequence]
			
# 		#reduce the sequences so that they are simpler on the next level:
# 		UNewsequences=[]
# 		UNewIndex=list(set(NewIndex))
# 		for sequence in Newsequences:
# 			usequence=[sequence[NewIndex.index(newind)] for newind in UNewIndex]
# 			UNewsequences=UNewsequences+[usequence]
		
# 		ax = fig.add_subplot(100*NestedLevels+11+k)
# 		AX=AX+[ax]
# 		#make a figure:
# 		NewSwitches.reverse()
# 		Newsequences.reverse()
# 		UNewsequences.reverse()
# 		AX[k],nest[k]=plot_switch_pattern(NewSwitches,UNewsequences,linewidth=linewidth,width=width,axis=AX[k],time=UNewIndex,xlabel=NestedNames[level-1],add_blocks=True)
		
# 	#now that we have contructed the plot, we want to connect lines between subplots
# 	#that indicate a nesting structure
   
# 	lines=[]
# 	for k in xrange(len(nest)-1):
# 		transFigure = fig.transFigure.inverted()
	
# 		coord1 = transFigure.transform(AX[k].transData.transform([nest[k][0],-.5]))
# 		coord2 = transFigure.transform(AX[k+1].transData.transform([nest[k+1][2]+BlockChoice[k],nest[k+1][4]]))
# 		coord3 = transFigure.transform(AX[k].transData.transform([nest[k][1],-.5]))
# 		coord4 = transFigure.transform(AX[k+1].transData.transform([nest[k+1][3]+BlockChoice[k],nest[k+1][4]]))
	
	
# 		line1 = matplotlib.lines.Line2D((coord1[0],coord2[0]),(coord1[1],coord2[1]),
# 										   transform=fig.transFigure,color='k',alpha=.3) 
# 		line2 = matplotlib.lines.Line2D((coord3[0],coord4[0]),(coord3[1],coord4[1]),
# 										   transform=fig.transFigure,color='k',alpha=.3)
# 		lines=lines+[line1,line2]									   
		
	
# 	fig.lines = lines
	
	
# 	#if not(save<0):
# 		#this pdf saving feature is not going to be available for the executable version.
# 		#pp = PdfPages(('C:\Users\Brendon\Documents\Python Scripts\Images\\'+save))
# 		#pp.savefig(bbox_inches='tight')
# 		#pp.close()
# 	#else:	
# 	return fig

		
	

#alright, lets see if I can generate a superblock by using nesting
#this sequence is similar to Gen I in that the AB vs BA and ABBA and BAAB sequences
#within the block changed from block to block, but here it is deterministic rather
#than random:
#NSwitches=[ Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='AB'),
#			Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='BA')]
#ESwitches=[ Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=1,configuration='ABBA'),
#			Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=1,configuration='BAAB')]
#ThetaSwitches=[ Switch('$\\tilde{\\theta}$',[-1,1],timescale=2,configuration='ABBA'),
#				Switch('$\\tilde{\\theta}$',[-1,1],timescale=2,configuration='BAAB')]
#BSwitches=[ Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=3,configuration='AB'),
#			Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=3,configuration='BA')]
#			
#Switches=[Switch('$\\tilde{\mathcal{N}}$',NSwitches,timescale=0,force_unique=False),
#		  Switch('$\\tilde{\mathcal{E}}$',ESwitches,timescale=0,force_unique=False),
#		  Switch('$\\tilde{\\theta}$',ThetaSwitches,timescale=0,force_unique=False),
#		  Switch('$\\tilde{\mathcal{B}}$',BSwitches,timescale=0,force_unique=False),
#		  Switch('$\\tilde{\mathcal{P}}$',[-1,1],timescale=0,force_unique=True),
#		  Switch('$\\tilde{\mathcal{R}}$',[-1,1],timescale=1,force_unique=True)]

#create the block switches:
#NSwitches=[ Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='AB'),
#			Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='BA')]
#ESwitches=[ Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=1,configuration='ABBA'),
#			Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=1,configuration='BAAB')]
#ThetaSwitches=[ Switch('$\\tilde{\\theta}$',[-1,1],timescale=2,configuration='ABBA'),
#				Switch('$\\tilde{\\theta}$',[-1,1],timescale=2,configuration='BAAB')]
#BSwitches=[ Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=3,configuration='AB'),
#			Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=3,configuration='BA')]
#
##construct the superblock behavior:			
#Switches=[Switch('$\\tilde{\mathcal{N}}$',NSwitches,timescale=0,force_unique=False,configuration=['random sequence',[[0],[1]]]),
#		  Switch('$\\tilde{\mathcal{E}}$',ESwitches,timescale=0,force_unique=False,configuration=['random sequence',[[0],[1]]]),
#		  Switch('$\\tilde{\\theta}$',ThetaSwitches,timescale=0,force_unique=False,configuration=['random sequence',[[0],[1]]]),
#		  Switch('$\\tilde{\mathcal{B}}$',BSwitches,timescale=0,force_unique=False),
#		  Switch('$\\tilde{\mathcal{P}}$',[-1,1],timescale=0,degeneracy=2,force_unique=True,configuration='rand(A,B)'),
#		  Switch('$\\tilde{\mathcal{L}}$',[-1,1],timescale=1,force_unique=True),
#		  Switch('$\\tilde{\mathcal{R}}$',[-1,1],timescale=2,force_unique=True),
#		  Switch('$\\tilde{\mathcal{G}}$',[-1,1],timescale=3,force_unique=True)]

def OpenFileAndExecute(FileName):
	"""Opens a file FileName, which has python code defining a switch array called Switches"""
	with open(FileName, 'r') as f:
		exec(f.read())
	return Switches
	#else:
	#	raise RuntimeError('the script must generate a switch array named Switches')
		
def SplitPathAndFile(FileName):
	Path=FileName[::-1][FileName[::-1].find('\\'):][::-1]
	Name=FileName[::-1].split('.')[1].split('\\')[0][::-1]
	return Path,Name	 
	 
def SaveSwitchPatternToFile(Switches,States,sequences,Statesequences,StateIndex,FileName):
	"""Saves the generated switch sequence to a text file for use by the experiment
	   This uses the simple systax that the filename that is saved is in the same
	   folder as the command, but has _Sequence appended to the name"""
	#generate a new filename:
	Path,Name=SplitPathAndFile(FileName)
	NewName=Name+'_StateSequence.txt'
	NewFileName=Path+NewName
	
	#save the switch sequence
	with open(NewFileName,'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=' ',
								quotechar='|', quoting=csv.QUOTE_MINIMAL)
		for i in xrange(len(Switches)):
			writer.writerow([Switches[i].name]+Statesequences[i])
	
	NewName=Name+'_Sequence.txt'
	NewFileName=Path+NewName		
	#save the switch sequence
	with open(NewFileName,'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=' ',
								quotechar='|', quoting=csv.QUOTE_MINIMAL)
		for i in xrange(len(Switches)):
			writer.writerow([Switches[i].name]+sequences[i])
	
	NewName=Name+'_Indices.txt'
	NewFileName=Path+NewName		
	#save the state indices:
	with open(NewFileName,'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=' ',
								quotechar='|', quoting=csv.QUOTE_MINIMAL)
		for i in xrange(len(StateIndex)):
			writer.writerow(StateIndex[i])
			
	NewName=Name+'_States.txt'
	NewFileName=Path+NewName		
	#save the switch sequence
	with open(NewFileName,'wb') as csvfile:
		writer = csv.writer(csvfile, delimiter=' ',
								quotechar='|', quoting=csv.QUOTE_MINIMAL)
		for i in xrange(len(Switches)):
			writer.writerow([Switches[i].name]+States[i])
			
def OpenSwitchPatternFromFile(FileName):
	Path,Name=SplitPathAndFile(FileName)
	NewFileNames=[Path+Name+'_States.txt',Path+Name+'_Sequence.txt',Path+Name+'_Indices.txt']
	
	y=[[]]*len(NewFileNames)
	for i in xrange(len(NewFileNames)):
		with open(NewFileNames[i], 'rb') as csvfile:
			reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
			
			for row in reader:
				if i==2:
					start=0
				else:
					start=1
				
				add=row[start:]
				if not(i==0):
					add=[int(s) for s in add]
						
				y[i]=y[i]+[add]
	return y[0],y[1],y[2]
	#these are States,sequences,StateIndex

	
def SaveFigureToFile(fig,FileName,show=False):
	Path=FileName[::-1][FileName[::-1].find('\\'):][::-1]
	Name=FileName[::-1].split('.')[1].split('\\')[0][::-1]
	NewName=Name+'_Figure.png'
	NewFileName=Path+NewName
	plt.savefig(NewFileName, bbox_inches='tight',dpi = 300)
	if not(show):
		plt.close()
   
   
##contruct the uberblock behavior:
##first, nest all of the existing switches:
#Switches=[switch.nest() for switch in Switches]
#
#Switches=Switches+[Switch('$|\mathcal{B}_z|$',['0B','1B','2B'],timescale=4,configuration=['sequence',[1,0,1,2]]),
#				   Switch('$|\mathcal{E}_z|$',['low','high'],timescale=5,degeneracy=3),
#				   Switch('$\hat{k}\cdot\hat{z}$',[-1,1],timescale=6,configuration='BA')]

#NSwitches=[ Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='rand(A,B)'),
#			Switch('$\\tilde{\mathcal{N}}$',[-1,1],timescale=0,configuration='AB')]
#ESwitches=[ Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=1,configuration='ABBA'),
#			Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=1,configuration='ABBA'),
#			Switch('$\\tilde{\mathcal{E}}$',[-1,1],timescale=0,configuration='BAAB')]
#ThetaSwitches=[ Switch('$\\tilde{\\theta}$',[-1,1],timescale=2,configuration='rand(ABBA,BAAB)'),
#				Switch('$\\tilde{\\theta}$',[-1,1],timescale=2,configuration='AB')]
#BSwitches=[ Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=3,configuration='AB'),
#			Switch('$\\tilde{\mathcal{B}}$',[-1,1],timescale=3,configuration='rand(A,B)')]
#
##construct the superblock behavior:			
#Switches=[Switch('$\\tilde{\mathcal{N}}$',NSwitches,timescale=0,force_unique=False,configuration=['random sequence',[[0],[1]]]),
#		  Switch('$\\tilde{\mathcal{E}}$',ESwitches,timescale=0,force_unique=False,configuration=['random sequence',[[0],[1]]]),
#		  Switch('$\\tilde{\\theta}$',ThetaSwitches,timescale=0,force_unique=False,configuration=['random sequence',[[0],[1]]]),
#		  Switch('$\\tilde{\mathcal{B}}$',BSwitches,timescale=0,force_unique=False),
#		  Switch('$\\tilde{\mathcal{P}}$',[-1,1],timescale=0,degeneracy=2,force_unique=True,configuration='rand(A,B)'),
#		  Switch('$\\tilde{\mathcal{L}}$',[-1,1],timescale=1,force_unique=True,configuration='rand(A,B)'),
#		  Switch('$\\tilde{\mathcal{R}}$',[-1,1],timescale=2,force_unique=True,configuration='rand(A,B)'),
#		  Switch('$\\tilde{\mathcal{G}}$',[-1,1],timescale=3,force_unique=True,configuration='rand(A,B)')]
##
###contruct the uberblock behavior:
###first, nest all of the existing switches:
#Switches=[switch.nest() for switch in Switches]
##
#Switches=Switches+[Switch('$|\mathcal{B}_z|$',['0B','1B','2B'],timescale=4,configuration=['random sequence',[[1,0,1,2],[0,1,1,2]]]),
#				   Switch('$|\mathcal{E}_z|$',['low','high'],timescale=5,degeneracy=3,configuration='rand(A,B)'),
#				   Switch('$\hat{k}\cdot\hat{z}$',[-1,1],timescale=6,configuration='BA')]				  

############# Create sequences and Plot Them #####################
#FileName='C:\Users\Brendon\Documents\Python Scripts\Experiment Switch Configuration\SwitchCommands.txt'
#Switches=OpenFileAndExecute(FileName)		  
#States,sequences,Statesequences,StateIndex=GenerateNestedStateSequence(Switches)
#SaveSwitchPatternToFile(Switches,States,sequences,Statesequences,StateIndex,FileName)
#fig=PlotNestedSuperblockSequence(States,sequences,StateIndex,BlockChoice=[2,0])
#SaveFigureToFile(fig,FileName)
#things to do:
#   1. create a switch file format 
#   2. create an output of commands to save to the database
#   3. create a figure output in some bitmap format
#   4. package this up for use on an experiment computer


############## COMMAND LINE FUNCTION ####################

#here is the command line function:
if __name__ == '__main__':
	AllowedFunctions=['generate_sequence','generate_plot']

	args=sys.argv
	args.pop(0)
	#print args
	#args=['generate_sequence','C:\Users\Brendon\switches\SwitchCommands.txt']
	#args=['generate_plot','C:\Users\Brendon\switches\SwitchCommands.txt','1,0']
	if args[0] in AllowedFunctions:
		if args[0]==AllowedFunctions[0]:
			if len(args)>1:
				FileName=str(args[1])
				Switches=OpenFileAndExecute(FileName)		  
				States,sequences,Statesequences,StateIndex=GenerateNestedStateSequence(Switches)
				SaveSwitchPatternToFile(Switches,States,sequences,Statesequences,StateIndex,FileName)
				fig=PlotNestedSuperblockSequence(States,sequences,StateIndex)
				SaveFigureToFile(fig,FileName,show=True)
				print 'generated sequence and plot successfully.'
			else:
				print 'you must provide a path corresponding to sequence generating instructions after generate_sequence.'
		elif args[0]==AllowedFunctions[1]:
			if len(args)>1:
				FileName=str(args[1])
				Switches=OpenFileAndExecute(FileName)
				States,sequences,StateIndex=OpenSwitchPatternFromFile(FileName)
				
				if len(args)>2:
					BlockChoice=args[2].split(',')
					BlockChoice=[int(ind) for ind in BlockChoice]
					if len(BlockChoice)<(len(StateIndex)-1):
						BlockChoice=BlockChoice+[0]*(len(StateIndex)-len(BlockChoice)-1)

					fig=PlotNestedSuperblockSequence(States,sequences,StateIndex,BlockChoice=BlockChoice)
				else:
					fig=PlotNestedSuperblockSequence(States,sequences,StateIndex)
					
				SaveFigureToFile(fig,FileName,show=True)
				print 'generated plot successfully'
			else:
				print 'you must provide a path corresponding to sequence generating instructions after generate_plot.'
		elif args[0]==AllowedFunctions[2]:
			if len(args)>1:
				print 'this function has not yet been implimented'
			else:
				print 'you must provide a path corresponding to sequence generating instructions after generate_sql_commands.'
	else:
		print 'command not recognized - please choose one of the following commands '+str(AllowedFunctions)