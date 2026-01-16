using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public class TypeRacersNetworkClient : INetworkClient
    {
        // The real implementation
        private NetworkStream networkStream;
        private readonly TcpClient realTcpClient;
        private string dataRecieved;
        public NetworkStream GetStream() => realTcpClient.GetStream();

        public TypeRacersNetworkClient()
        {
            realTcpClient = new TcpClient();
        }

        public TypeRacersNetworkClient(TcpClient client)
        {
            realTcpClient = client;
        }

        public void Close()
        {
            realTcpClient.Close();
        }

        public void Dispose()
        {
            realTcpClient.Dispose();
        }

        public void Write(IMessage message)
        {
            networkStream = realTcpClient.GetStream();
            var toSend = message.ToByteArray();
            networkStream.Write(toSend, 0, toSend.Length);
        }

        public string Read()
        {

            if (realTcpClient.Connected)
            {
                networkStream = realTcpClient.GetStream();
                byte[] buffer = new byte[1024];
                int bytesRead = networkStream.Read(buffer, 0, buffer.Length);
                dataRecieved = Encoding.ASCII.GetString(buffer, 0, bytesRead);
                while (!dataRecieved.Contains("#"))
                {
                    bytesRead = networkStream.Read(buffer, 0, 1024);
                    dataRecieved += Encoding.ASCII.GetString(buffer, dataRecieved.Length, bytesRead);
                }

                string completeMessage = dataRecieved.Substring(0, dataRecieved.IndexOf('#'));
                dataRecieved.Remove(0, completeMessage.Length - 1);

                return completeMessage;
            }
            return string.Empty;
        }
    }
}