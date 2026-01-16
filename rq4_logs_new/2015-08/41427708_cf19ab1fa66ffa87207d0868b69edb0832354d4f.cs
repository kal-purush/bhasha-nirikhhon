using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO.Ports;
using System.Net.Sockets;
using System.Net;
using System.Diagnostics;
using System.Threading.Tasks;

namespace DylosDetector
{
	class Program
	{
		private static string graphiteAddress;
		private static int graphitePort;

		public static void Main(string[] args)
		{
			string[] serialPortNames = SerialPort.GetPortNames();
			if (serialPortNames.Count<string>() <= 0)
			{
				Debug.Print("Uh, there were no active serial ports... program aborting");
				return;
			}

			string targetSerialPort = serialPortNames[0]; // Eh, just pick the first one

			SerialPort particleSensorSeiralPort = new SerialPort(targetSerialPort);
			Console.WriteLine("Opening Serial Port: " + targetSerialPort);

			particleSensorSeiralPort.BaudRate = 9600;
			particleSensorSeiralPort.Parity = Parity.None;
			particleSensorSeiralPort.StopBits = StopBits.One;
			particleSensorSeiralPort.DataBits = 8;
			particleSensorSeiralPort.Handshake = Handshake.None;
			particleSensorSeiralPort.DataReceived += new SerialDataReceivedEventHandler(DataReceivedHandler);

			particleSensorSeiralPort.Open();

			Console.WriteLine("Press any key to close...");
			Console.WriteLine();
			Console.ReadKey();
			particleSensorSeiralPort.Close();
		}

		private static void DataReceivedHandler(object sender, SerialDataReceivedEventArgs e)
		{
			SerialPort sp = (SerialPort)sender;
			string indata = sp.ReadExisting();
			indata = indata.Trim();
			string[] inDataArray = indata.Split(',');

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

				sendingSocket.Send(Encoding.UTF8.GetBytes(smallParticleMessage));
				sendingSocket.Send(Encoding.UTF8.GetBytes(largeParticleMessage));

				sendingSocket.Close();
			}
		}
	}
}