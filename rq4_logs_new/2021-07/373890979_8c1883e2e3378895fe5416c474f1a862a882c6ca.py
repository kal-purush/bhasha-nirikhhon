import time
import serial
import serial.tools.list_ports as serports
import PySimpleGUI as psg

psg.theme('DarkTanBlue')

class GUI_serialMonitor:
    def __init__(self):
        self.cols1 = [
            [
                psg.Text("Transmit Data"),
                psg.In(
                    size=(85, 1), key="-TXD-",
                    background_color="white", text_color="black",
                ),
                psg.Button("Send", bind_return_key=True),
            ],
            [
                psg.Text("Serial Window"),
                psg.Multiline(
                    autoscroll=True, size=(85, 35), key="-RXD/LOC-",
                    background_color="black", text_color="white",
                    write_only=True
                )
            ],
        ]

        self.cols2 = [
            [
                psg.Text("No COM ports available!\nClick on GO to exit",
                key="-TXT-"),
            ],
            [
                psg.Text("COM"),
                psg.Combo(
                    values=[], size=(30,1), key="-COM-",
                    background_color="white", text_color="black",
                    disabled=True, visible=False
                    ),
            ],
            [
                psg.Text("Baudrate"),
                psg.Combo(
                    values=[4800, 9600, 19200, 115200], size=(10,1),
                    background_color="white", text_color="black",
                    default_value=9600, key="-BAUD-",
                    disabled=True, visible=False
                )
            ],
            [
                psg.Button("GO", bind_return_key=True)
            ]
        ]

        self.layout1 = [
            [
                psg.Column(self.cols1),
            ]
        ]

        self.layout2 = [
            [
                psg.Column(self.cols2),
            ]
        ]

        self.window1 = psg.Window(
            "Serial Monitor for 8051 Microcontroller",
             self.layout1, finalize=True)
        self.window2 = psg.Window(
            "Port Setup", self.layout2, size=(250,150), finalize=True
        )

        self.sp = None

        self.serial_count = 0
        self.local_count = 0
        self.event = None
        self.values = None

        self.com = None
        self.baud = None

    def portList(self):
        ports = serports.comports()
        comlst = []
        if len(ports) > 0:
            for port in ports:
                comlst.append(port.device)
            self.window2["-COM-"].update(values=comlst)
            self.window2["-TXT-"].update("COM port(s) available!!")
            self.window2["-COM-"].update(disabled=False)
            self.window2["-COM-"].update(visible=True)
            self.window2["-BAUD-"].update(disabled=False)
            self.window2["-BAUD-"].update(visible=True)

    def portSetup(self):
        com = None
        baud = None
        self.portList()
        while True:
            event, values = self.window2.read(timeout=0.5)
            if event == "Exit" or event == psg.WIN_CLOSED:
                break
            if event == "GO":
                com = values["-COM-"]
                baud = values["-BAUD-"]
                break
        self.window2.close()
        return com, baud

    def sendTX(self, str_data, sleep_time=0.025):
        try:
            for char in str_data:
                self.sp.write(char.encode())
                time.sleep(sleep_time)
            return "(Sent Successfully) :  " + str_data
        except:
            return "   (Failed to Send) :  " + str_data

    def recvRX(self):
        dataRX = "(No data)"
        try:
            serialstr = self.sp.readline()
            d = serialstr.decode('ascii')
            dataRX = "(Recieved): " + d
            self.serialDisp(dataRX, serial=True)
        except:
            print("failed recieve")

    def SM_start(self):
        start = "*** Serial Monitor for Microcontroller, Port = "
        initStr = start + self.com + ", baudrate = " +str(self.baud) + " ***"
        self.serialDisp(initStr, serial=False)
        while True:
            if self.sp.in_waiting > 0:
                self.recvRX()
            self.event, self.values = self.window1.read(timeout=0.5)
            if self.event == "Exit" or self.event == psg.WIN_CLOSED:
                break
            if self.event == "Send":
                dataTX = self.values["-TXD-"]
                status = self.sendTX(dataTX)
                self.window1["-TXD-"].update('')
                self.serialDisp(status, serial=False)
        self.window1.close()

    def serialDisp(self, payload, serial=True):
        if payload != "(No data)":
            if serial:
                ds = "[Serial IN: " + str(self.serial_count) + "] >> "
                self.serial_count += 1
                self.nxtLine = True
            else:
                ds = "[Serial OUT: " + str(self.local_count) + "] >> "
                self.local_count += 1
            self.window1["-RXD/LOC-"].print(ds, payload, end='\n')


def main():
    sm = GUI_serialMonitor()
    sp_available = False
    sm.com, sm.baud = sm.portSetup()
    if sm.com != None:
        if sm.com.startswith("COM"):
            sm.sp = serial.Serial(port=sm.com, baudrate=sm.baud, bytesize=8, timeout=2, stopbits=1)
            sm.SM_start()
    sm.window1.close()


if __name__ == "__main__":
    main()