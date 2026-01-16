from operator import attrgetter
from ryu.app import simple_switch_13
from ryu.controller import ofp_event
from ryu.controller.handler import MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.lib import hub

import coloredlogs, logging
import math

from pylab import *
import matplotlib.pyplot  as pyplot
a = [ pow(10,i) for i in range(10) ]
fig = pyplot.figure()
ax = fig.add_subplot(2,1,1)

############################ Initializing global variables
stage = 0
a = []				#for flow list
update = 0			# update or new flow flag
############################

coloredlogs.install()

class Navid(simple_switch_13.SimpleSwitch13):

	def __init__(self, *args, **kwargs):
		super(Navid, self).__init__(*args, **kwargs)
		self.datapaths = {}
		self.monitor_thread = hub.spawn(self._monitor)

	@set_ev_cls(ofp_event.EventOFPStateChange,
				[MAIN_DISPATCHER, DEAD_DISPATCHER])
	def _state_change_handler(self, ev):
		datapath = ev.datapath
		if ev.state == MAIN_DISPATCHER:
			if datapath.id not in self.datapaths:
				self.logger.debug('register datapath: %016x', datapath.id)
				self.datapaths[datapath.id] = datapath
		elif ev.state == DEAD_DISPATCHER:
			if datapath.id in self.datapaths:
				self.logger.debug('unregister datapath: %016x', datapath.id)
				del self.datapaths[datapath.id]

	def _monitor(self):
		while True:
			for dp in self.datapaths.values():
				self._request_stats(dp)
			hub.sleep(2)

	def _request_stats(self, datapath):
		self.logger.debug('send stats request: %016x', datapath.id)
		ofproto = datapath.ofproto
		parser = datapath.ofproto_parser

		req = parser.OFPFlowStatsRequest(datapath)
		datapath.send_msg(req)

		req = parser.OFPPortStatsRequest(datapath, 0, ofproto.OFPP_ANY)
		datapath.send_msg(req)

	@set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
	def _flow_stats_reply_handler(self, ev):
		
		global a			#for flow list
		global update			# update or new flow flag
		global stage
		b = []				#for individual flow record
		c = []				#for packet flow stats
		d = []				#for byte flow stats
		e = []				# AVG for packets
		f = []				# AVG for bytes
		g = []				# trend of changes for AVG of packets
		h = []				# trend of changes for AVG of bytes
		f_index	= 0			#
		f_array = []			#
		p_trend = 0
		b_trend = 0

		body = ev.msg.body
		#self.logger.critical('flowID      In-Port              DST          Out-Port    packets      bytes')
		#self.logger.critical('--------    -------      ----------------      ---------   -------      -----')
		for stat in sorted([flow for flow in body if flow.priority == 1], key=lambda flow: (flow.match['in_port'], flow.match['eth_dst'])):
			#self.logger.warning('%04x %8x %17s %8x %8d %8d', ev.msg.datapath.id, stat.match['in_port'], stat.match['eth_dst'], stat.instructions[0].actions[0].port, stat.packet_count, stat.byte_count)
###################################
			
			flowid = str(ev.msg.datapath.id) + str(stat.instructions[0].actions[0].port)
			update = 0	#reset update for next flow
			stage = 0	#reset alarm stage for next flow
			#print 'Size of main array is '+str(len(a))		#Printing main array before start
			#for f_index, f_array in enumerate(a):
				#self.logger.info(f_array)
			#print a[0]
			#print flowid
			#print a[3]
			#print stat.match['eth_dst']
			#print update

			for f_index, f_array in enumerate(a):
				if (f_array[0] == flowid and f_array[3] == stat.match['eth_dst']):
					self.logger.info ('UPDATE flow %s', flowid)
					pstat = 0
					bstat = 0
					pavg = 0
					bavg = 0		
					f_array[4].append(stat.packet_count)
					f_array[5].append(stat.byte_count)
					p_stat= sum(f_array[4])/len(f_array[4])		#average of packet stats
					b_stat = sum(f_array[5])/len(f_array[5])	#average of byte stats
					if (len(f_array[6]) >= 1):
						p_trend = abs(f_array[6][-1] - p_stat)		#trend of packet averages changes
					if (len(f_array[7]) >= 1):
						b_trend = abs(f_array[7][-1] - b_stat)		#trend of byte averages changes
					#print p_stat
					#print b_stat
					#print p_trend
					#print b_trend
					f_array[6].append(p_stat)
					f_array[7].append(b_stat)
					f_array[8].append(p_trend)
					f_array[9].append(b_trend)
					if (len(f_array[4]) >= 20):			#maintain 20 values in packet list
						del f_array[4][0]			#maintain LIFO order
					if (len(f_array[5]) >= 20):			#maintain 20 values in byte list
						del f_array[5][0]
					if (len(f_array[6]) >= 20):			#maintain 20 values in packet avg list
						del f_array[6][0]
					if (len(f_array[7]) >= 20):			#maintain 20 values in byte avg list
						del f_array[7][0]
					if (len(f_array[8]) >= 20):			#maintain 20 values in byte avg change trend
						del f_array[8][0]
					if (len(f_array[8]) >= 10):			#initiate anomaly detection if 10 values populaed with staging ratio of 1 for packet count
						#_anomaly(f_array[0],f_array[8],1)
						#self.logger.error ('Current value: %d', f_array[8][-1])
						avg = sum(f_array[8])/len(f_array[8])
						#self.logger.error ('AVG: %d EXP-AVG: %d', avg, math.pow(f_array[8][-2], 2))
						if (f_array[8][-1] > math.pow(avg, 2)):	#evaluation of exponential growth in trends
							stage += 1
							self.logger.error ('Packet Elevated STAGE %s', f_array[0])						
							if (stage >= 2):			#level of consensus for trigerring alarm
								self.logger.critical ('Packet Triggered ALARM %s', f_array[0])
								#####
								fig = pyplot.figure()
								ax = fig.add_subplot(2,1,1)
								line, = ax.plot(f_array[8], color='blue', lw=2)
								show()
								#####	
					if (len(f_array[9]) >= 20):			#maintain 20 values in byte avg change trend
						del f_array[9][0]
					if (len(f_array[9]) >= 10):			#initiate anomaly detection if 10 values populaed with staging ratio of 2 for byte transmitted
						#_anomaly(f_array[0],f_array[9],2)
						#self.logger.error ('Current value: %d', f_array[9][-1])
						avg = sum(f_array[9])/len(f_array[9])
						#self.logger.error ('AVG: %d EXP-AVG: %d', avg, math.pow(f_array[9][-2], 2))
						if (f_array[9][-1] > math.pow(f_array[9][-2], 2)):	#evaluation of exponential growth in trends
							stage += 1
							self.logger.error ('Byte Elevated STAGE for %s', f_array[0])
							if (stage >= 2):			#level of consensus for trigerring alarm
								self.logger.critical ('Byte Triggered ALARM %s', f_array[0])
								#####
								fig = pyplot.figure()
								ax = fig.add_subplot(2,1,1)
								line, = ax.plot(f_array[9], color='red', lw=2)
								show()
								#####
					update = 1
			
			if (update == 0):
				self.logger.critical ('NEW Size of main array is: %s ', str(len(a)))		#Printing main array before start	
				#print f_index
				b = [flowid,stat.match['in_port'],stat.instructions[0].actions[0].port,stat.match['eth_dst']]	#for individual flow record
				c = [stat.packet_count]	#for packet flow stats
				d = [stat.byte_count]	#for byte flow stats
				e = []	# AVG for packets
				f = []	# AVG for bytes
				g = []	# trend of changes for AVG of packets
				h = []	# trend of changes for AVG of bytes
				b.append(c)
				b.append(d)
				b.append(e)
				b.append(f)
				b.append(g)
				b.append(h)
				a.append(b)
				print f_array
				update = 0
###################################