using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Common;
namespace Client
{
    public class PlayerInfo : IPlayer, IMessage
    {
        TcpClient client;
        NetworkStream stream;
        public string Name { get  ; set ; }
        public int Place { get; set; } 
        public bool Finnished { get ; set ; }
        public int WPMProgress { get; set; } 
        public int CompletedTextPercentage { get ; set ; }

        public PlayroomInfo PlayroomInfo { get; set; }

        public PlayerInfo(TcpClient tcpClient)
        {
            client = tcpClient;
        }

        public void CheckReceivedData(string dataReceived)
        {
            //progress and slider progress
            string progress = dataReceived.Substring(0, dataReceived.IndexOf('$'));

            var progressInfoAndPlayerRoomInfo = progress.Split('&');

            string username = dataReceived.Substring(dataReceived.IndexOf('$') + 1);

            Name = username.Substring(0, username.Length - 1);
            UpdateInfo(Convert.ToInt32(progressInfoAndPlayerRoomInfo[0]), Convert.ToInt32(progressInfoAndPlayerRoomInfo[1]));
        }

        public string Message()
        {
            return WPMProgress + "&" + CompletedTextPercentage + "$" + Name + "#";
        }

        public void Read()
        {
            stream = client.GetStream();
            byte[] inStream = new byte[client.ReceiveBufferSize];
            int read = stream.Read(inStream, 0, inStream.Length);
            string receivedData = Encoding.ASCII.GetString(inStream, 0, read);

            while (!receivedData[read - 1].Equals('#'))
            {
                read = stream.Read(inStream, 0, inStream.Length);
                receivedData += Encoding.ASCII.GetString(inStream, receivedData.Length, read);
            }

            CheckReceivedData(receivedData);
        }

        public void UpdateInfo(int wpmProgress, int completedText)
        {
            WPMProgress = wpmProgress;
            CompletedTextPercentage = completedText;
            if (completedText == 100)
            {
                Finnished = true;
                Place = PlayroomInfo.Place;
            }
        }

        public void UpdateOpponents()
        {
            throw new NotImplementedException();
        }

        public void Write()
        {
            stream = client.GetStream();
            while (true)
            {
                byte[] toSend = Encoding.ASCII.GetBytes(Message());
                stream.Write(toSend, 0, toSend.Length);
            }
        }
    }
}