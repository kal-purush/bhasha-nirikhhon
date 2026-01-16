using BrawlTCG_alpha.Logic.Cards;
using BrawlTCG_alpha.Visuals;
using Microsoft.VisualBasic.Devices;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace BrawlTCG_alpha.Logic
{
    public class NetworkManager
    {
        // Properties
        private TcpClient _client;
        private bool _isHost, _myTurn;
        public StreamWriter Writer { get; private set; }
        public StreamReader Reader { get; private set; }

        // Methods
        public NetworkManager(TcpClient client, bool isHost, bool myTurn)
        {
            // Parameters
            _client = client;
            _isHost = isHost;
            _myTurn = myTurn;

            // Setup Network
            NetworkStream stream = client.GetStream();
            Reader = new StreamReader(stream);
            Writer = new StreamWriter(stream) { AutoFlush = true };
        }

        public void SendMessageToPeer(string message)
        {
            try
            {
                if (Writer != null)
                {
                    Writer.WriteLine(message);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to send message: {ex.Message}");
            }
        }
    }
}