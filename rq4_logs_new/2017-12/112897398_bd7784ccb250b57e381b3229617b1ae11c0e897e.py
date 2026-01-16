# test script

import tkinter
import threading
import websocket
import json

class geminiApp(tkinter.Tk):
	def __init__(self,parent):
		tkinter.Tk.__init__(self,parent)
		self._timer = None
		self._ws = None
		self.parent = parent
		self.initializeUI()
		self.initializeWS()
		self.setLabel()
		# self.startTimer()

	def initializeUI(self):
		self.grid()

		self.btcLabel = tkinter.StringVar()
		label = tkinter.Label(self, textvariable=self.btcLabel, anchor='w', fg='white', bg='black')
		label.grid(column=0, row=0, columnspan=2, sticky='EW')

		self.ethLabel = tkinter.StringVar()
		label = tkinter.Label(self, textvariable=self.ethLabel, anchor='w', fg='white', bg='black')
		label.grid(column=2, row=0, columnspan=2, sticky='EW')

		priceLabel = tkinter.Label(self, text='Price:', width=6, anchor='e', fg='white', bg='black')
		priceLabel.grid(column=0, row=1, sticky='EW')

		self.priceVar = tkinter.StringVar()
		priceEntry = tkinter.Entry(self, textvariable=self.priceVar)
		priceEntry.grid(column=1, row=1, sticky='EW')

		amtLabel = tkinter.Label(self, text='Amt:', width=4, anchor='e', fg='white', bg='black')
		amtLabel.grid(column=2, row=1, sticky='EW')
		self.amtVar = tkinter.StringVar()
		amtEntry = tkinter.Entry(self, textvariable=self.amtVar)
		amtEntry.grid(column=3, row=1, sticky='EW')

		buyBtn = tkinter.Button(self,text='Buy', command=self.OnButtonClick)
		buyBtn.grid(column=4, row=1)

		sellBtn = tkinter.Button(self,text='Sell', command=self.OnButtonClick)
		sellBtn.grid(column=5, row=1)


		self.grid_columnconfigure(0,weight=1)
		self.resizable(True,False)
		self.update()
		self.geometry(self.geometry())
		# self.entry.focus_set()
		self.protocol('WM_DELETE_WINDOW', self.onClose)

	def initializeWS(self):
		wst = threading.Thread(target=self.wsConnection)
		wst.daemon = True
		wst.start()

	def wsConnection(self):
		self._ws = websocket.WebSocketApp("wss://api.gemini.com/v1/marketdata/BTCUSD", on_message=self.onMessage, on_error=self.onError, on_close=self.onClose)
		self._ws.run_forever(ping_interval=5)

	def onMessage(self, ws, message):
		data = json.loads(message)
		event = data['events'][0]
		if event['type'] == 'trade':
			self.setLabel(event['price'])

	def onError(self, ws, error):
		pass

	def setLabel(self, btc='-', eth='-'):
		self.btcLabel.set('BTC: ' + btc)
		self.ethLabel.set('ETH: ' + eth)

	def onClose(self):
		self._ws.keep_running = False;
		self.destroy()

	# def _apiCall(self):
	# 	self.start()
	# 	self.setLabel('x', 'y')

	# def startTimer(self):
	# 	self._timer = threading.Timer(1.0, self._apiCall)
	# 	self._timer.start()

	def OnButtonClick(self):
		pass

	def OnPressEnter(self,event):
		pass

if __name__ == "__main__":
	app = geminiApp(None)
	app.title('GeminiApp')
	app.minsize(width=300, height=0)
	app.mainloop()