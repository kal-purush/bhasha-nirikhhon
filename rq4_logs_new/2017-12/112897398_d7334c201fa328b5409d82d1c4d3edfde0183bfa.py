# test script

import tkinter
import threading

class geminiApp(tkinter.Tk):
	def __init__(self,parent):
		tkinter.Tk.__init__(self,parent)
		self._timer = None
		self.parent = parent
		self.initialize()
		self.start()

	def initialize(self):
		self.grid()

		self.btcLabel = tkinter.StringVar()
		label = tkinter.Label(self, textvariable=self.btcLabel, anchor='w', fg='white', bg='black')
		label.grid(column=0, row=0, sticky='EW')

		self.ethLabel = tkinter.StringVar()
		label = tkinter.Label(self, textvariable=self.ethLabel, anchor='w', fg='white', bg='black')
		label.grid(column=1, row=0, columnspan=2, sticky='EW')

		self.entryVar = tkinter.StringVar()
		self.entry = tkinter.Entry(self, textvariable=self.entryVar)
		self.entry.grid(column=0, row=1, sticky='EW')
		self.entry.bind("<Return>", self.OnPressEnter)
		self.entryVar.set(u'buy price')

		buyBtn = tkinter.Button(self,text='Buy', command=self.OnButtonClick)
		buyBtn.grid(column=1, row=1)

		sellBtn = tkinter.Button(self,text='Sell', command=self.OnButtonClick)
		sellBtn.grid(column=2, row=1)


		self.grid_columnconfigure(0,weight=1)
		self.resizable(True,False)
		self.update()
		self.geometry(self.geometry())
		self.entry.focus_set()
		self.protocol('WM_DELETE_WINDOW', self.onClose)

	def setLabel(self, btc, eth):
		self.btcLabel.set('BTC: ' + btc)
		self.ethLabel.set('ETH: ' + eth)

	def start(self):
		self._timer = threading.Timer(1.0, self._apiCall)
		self._timer.start()

	def onClose(self):
		self._timer.cancel()
		self.destroy()

	def _apiCall(self):
		self.start()
		self.setLabel('x', 'y')

	def OnButtonClick(self):
		pass

	def OnPressEnter(self,event):
		pass

if __name__ == "__main__":
	app = geminiApp(None)
	app.title('GeminiApp')
	app.mainloop()