using System;
using System.Linq;
using System.Text;
using System.IO.Ports;
using System.Net.Sockets;
using System.Diagnostics;

namespace DylosDetector
{
    class DylosGraphiteConnection
    {
        private static string graphiteAddress;
        private static int graphitePort;

        private SerialPort particleSensorSerialPort = null;

        private System.Windows.Forms.TextBox logBox = null;

        public DylosGraphiteConnection(System.Windows.Forms.TextBox textbox)
        {
            logBox = textbox;
        }

        ~DylosGraphiteConnection()
        {
            Disconnect();
        }

        private static void DataReceivedHandler(object sender, SerialDataReceivedEventArgs e)
        {
            SerialPort sp = (SerialPort)sender;
            string indata = sp.ReadExisting();
            indata = indata.Trim();
            string[] inDataArray = indata.Split(',');

            DylosGraphiteConnection dylos = sender as DylosGraphiteConnection;

            if (inDataArray.Count<string>() > 1)
            {
                Console.WriteLine("---Data Received--- ");
                Console.WriteLine("Small Particle Count = " + inDataArray[0]);
                Console.WriteLine("Large Particle Count = " + inDataArray[1]);

                Socket sendingSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

                sendingSocket.Connect(graphiteAddress, graphitePort);

                TimeSpan t = (DateTime.UtcNow - new DateTime(1970, 1, 1));
                int currentEpochTime = (int)t.TotalSeconds;
                string smallParticleMessage = string.Format("lwells.dylos.small_particle {0} {1}\n", inDataArray[0], currentEpochTime);
                string largeParticleMessage = string.Format("lwells.dylos.large_particle {0} {1}\n", inDataArray[1], currentEpochTime);

                Debug.Print(smallParticleMessage);
                Debug.Print(largeParticleMessage);

                dylos.logBox.Text += smallParticleMessage;
                dylos.logBox.Text += largeParticleMessage;

                sendingSocket.Send(Encoding.UTF8.GetBytes(smallParticleMessage));
                sendingSocket.Send(Encoding.UTF8.GetBytes(largeParticleMessage));

                sendingSocket.Close();
            }
        }

        public bool Connect()
        {
            string[] serialPortNames = SerialPort.GetPortNames();
            if (serialPortNames.Count<string>() <= 0)
            {
                Debug.Print("Uh, there were no active serial ports... program aborting");
                return false;
            }

            string targetSerialPort = serialPortNames[0]; // Eh, just pick the first one

            SerialPort particleSensorSerialPort = new SerialPort(targetSerialPort);
            Console.WriteLine("Opening Serial Port: " + targetSerialPort);

            particleSensorSerialPort.BaudRate = 9600;
            particleSensorSerialPort.Parity = Parity.None;
            particleSensorSerialPort.StopBits = StopBits.One;
            particleSensorSerialPort.DataBits = 8;
            particleSensorSerialPort.Handshake = Handshake.None;
            particleSensorSerialPort.DataReceived += new SerialDataReceivedEventHandler(DataReceivedHandler);

            particleSensorSerialPort.Open();

            //Console.WriteLine("Press any key to close...");
            //Console.WriteLine();
            //Console.ReadKey();
            //particleSensorSerialPort.Close();

            return true;
        }

        public void Disconnect()
        {
            if (particleSensorSerialPort != null && particleSensorSerialPort.IsOpen)
            {
                particleSensorSerialPort.Close();
            }
        }
    }
}