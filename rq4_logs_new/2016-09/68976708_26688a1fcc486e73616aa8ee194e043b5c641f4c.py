#!/usr/bin/python

import pygtk
import gtk

li = 'LISTEN'
ta = 'TALK  '
cli = 'client'
ser = 'server'

class Config:

	def mc_select(self, widget, data=None):
		global conn_typ, conn_typ2
		conn_typ = 'multicast'
		conn_typ2 = 'group '

	def udp_select(self, widget, data=None):
		global conn_typ, conn_typ2
		conn_typ = 'udp      '
		conn_typ2 = 'ipaddr'

	def eth0_select(self, widget, data=None):
		global eth
		eth = 'eth0'

	def eth1_select(self, widget, data=None):
		global eth
		eth = 'eth1'

	def eth2_select(self, widget, data=None):
		global eth
		eth = 'eth2'

	def filt_select(self, widget, data=None):
		global filt
		filt = 'filter'

	def nofilt_select(self, widget, data=None):
		global filt
		filt = 'nofilter'

	def delete_event(self, widget, event, data=None):
		gtk.main_quit()
		return False	

	def printconfig(self, widget, data=None):
		print " {} {} {} {} {} {} {} {} " .format (li, ta, cli, ser, conn_typ, conn_typ2, eth, filt)
	
	def createtyp(self, vertical, title, spacing, layout):
		frame = gtk.Frame(title)
	
		bbox = gtk.VButtonBox()
		
		bbox.set_border_width(5)
		frame.add(bbox)

		bbox.set_layout(layout)
		bbox.set_spacing(spacing)

		button1 = gtk.RadioButton(None, "None")
		bbox.add(button1)

		button1 = gtk.RadioButton(button1, "Multicast")
		button1.connect("toggled", self.mc_select)
		bbox.add(button1)

		button1 = gtk.RadioButton(button1, "UDP")
		button1.connect("toggled", self.udp_select)
		bbox.add(button1)

		return frame

	def createeth(self, vertical, title, spacing, layout):
		frame = gtk.Frame(title)
	
		bbox = gtk.VButtonBox()
	
		bbox.set_border_width(5)
		frame.add(bbox)

		bbox.set_layout(layout)
		bbox.set_spacing(spacing)

		button2 = gtk.RadioButton(None, "None")
		bbox.add(button2)

		button2 = gtk.RadioButton(button2, "eth0")
		button2.connect("toggled", self.eth0_select)
		bbox.add(button2)

		button2 = gtk.RadioButton(button2, "eth1")
		button2.connect("toggled", self.eth1_select)
		bbox.add(button2)

		button2 = gtk.RadioButton(button2, "eth2")
		button2.connect("toggled", self.eth2_select)
		bbox.add(button2)

		return frame
		
	def filters(self, vertical, title, spacing, layout):
		frame = gtk.Frame(title)

		bbox = gtk.VButtonBox()

		bbox.set_border_width(5)
		frame.add(bbox)
		
		bbox.set_layout(layout)
		bbox.set_spacing(spacing)

		button3 = gtk.RadioButton(None, "None")
		bbox.add(button3)

		button3 = gtk.RadioButton(button3, "Filter")
		button3.connect("toggled", self.filt_select)
		bbox.add(button3)
		
		button3 = gtk.RadioButton(button3, "No Filter")
		button3.connect("toggled", self.nofilt_select)
		bbox.add(button3)

		return frame

	def printbutton(self, vertical, spacing, layout):
		frame = gtk.Frame()

		bbox = gtk.VButtonBox()
		
		bbox.set_border_width(5)
		frame.add(bbox)

		bbox.set_layout(layout)
		bbox.set_spacing(spacing)
		
		button4 = gtk.Button("Print")
		button4.connect("clicked", self.printconfig)
		bbox.add(button4)

		return frame


	def __init__(self):
		window = gtk.Window(gtk.WINDOW_TOPLEVEL)
		window.set_title("Configuration")
		
		window.connect("delete_event", self.delete_event)
		
		window.set_border_width(10)

		main_box = gtk.VBox(False, 0)
		window.add(main_box)
		
		frame_vert = gtk.Frame("Thread 1")
		main_box.pack_start(frame_vert, True, True, 10)

		hbox = gtk.HBox(False, 10)
		hbox.set_border_width(10)
		frame_vert.add(hbox)	

		hbox.pack_start(self.createtyp(False, "Connection Type", 5, gtk.BUTTONBOX_START), True, True, 5)

		hbox.pack_start(self.createeth(False, "Ethernet Setting", 5, gtk.BUTTONBOX_START), True, True, 5)

		hbox.pack_start(self.filters(False, "Filter Setting", 5, gtk.BUTTONBOX_START), True, True, 5)

		frame_vert2 = gtk.Frame()
		main_box.pack_start(frame_vert2, True, True, 10)

		hbox2 = gtk.HBox(False, 10)
		hbox2.set_border_width(10)
		frame_vert2.add(hbox2)
	
		hbox2.pack_start(self.printbutton(False, 5, gtk.BUTTONBOX_SPREAD), True, True, 5)

		window.show_all()


def main():
	gtk.main()
	return 0

if __name__ == "__main__":
	Config()
	main()