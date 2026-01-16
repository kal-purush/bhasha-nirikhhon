
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;

namespace TcpEchoClient
{
	class TcpEchoClient
	{
		static void Main(string[] args)
		{
			Console.WriteLine("Starting echo client...");

			int port = 1234;
			TcpClient client = new TcpClient("localhost", port);
			NetworkStream stream = client.GetStream();
			StreamReader reader = new StreamReader(stream);
			StreamWriter writer = new StreamWriter(stream) { AutoFlush = true };

			while (true)
			{
				Console.Write("Enter to send: ");
				string lineToSend = Console.ReadLine();
				Console.WriteLine("Sending to server: " + lineToSend);
				writer.WriteLine(lineToSend);
				string lineReceived = reader.ReadLine();
				Console.WriteLine("Received from server: " + lineReceived);
			}
		}
	}
}