using System;
using System.IO.Ports;
using System.Threading;

namespace CerealFileTransfer {
    class Network {
        private String portName;
        private Int32 baudrate;
        private Int32 dataBits;
        private StopBits stopBits;  // Der Anzahl der Stoppbits (Stopbits.One, StopBits.OnePointFive, StopBits.Two)
        private Parity parity;      // Festlegung der Parität (Parity.Even, Parity.Mark, Parity.None, Parity.Odd, Parity.Space)
        private Int32 bufferSize;
        private Int32 packageSize;
        private SerialPort serial;
        private Boolean isReceaving;
        private Thread receieveThread;

        public Network(Int32 baudrate, Int32 bufferSize, Int32 packageSize) {
            this.portName = SerialPort.GetPortNames()[0];
            this.baudrate = baudrate;
            this.dataBits = 8;
            this.stopBits = StopBits.One;
            this.parity = Parity.None;
            this.bufferSize = bufferSize;
            this.packageSize = packageSize;

            this.serial = new SerialPort() {
                PortName = portName,
                BaudRate = baudrate,
                DataBits = dataBits,
                StopBits = stopBits,
                Parity = parity,
                ReadBufferSize = bufferSize,
                WriteBufferSize = bufferSize / 2,
                DtrEnable = true           
            };
        }

        public void Open() { try { this.serial.Open(); } catch (Exception ex) { throw ex; } }

        public Byte[][] GetPackage(Int32 count) {
            Byte[][] package = new Byte[count][];
            Byte[] buffer = new Byte[this.packageSize];

            for (int i = 0; i < count; i++) {
                this.serial.Read(buffer, 0, 1);
                package[i] = buffer;
            }

            return package;
        }

        public void SendPackage(Byte[][] package) {            
            for (int i = 0; i < package.Length; i++) {
                //while (this.serial.CtsHolding);
                this.serial.Write(package[i], 0, 1);
            }
        }
    }
}