using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace ChessAI.ChatServerAndClient
{
    internal class CommunicateSkeleton
    {
        // This class is used to summarize skeleton of all chat forms
        // This class is not used in the project

        // Init comm
        private Communication comm;
        string serverIP = ChessAI.ChatServerAndClient.Constants.serverIP;
        int serverPort = ChessAI.ChatServerAndClient.Constants.serverPort;

        public CommunicateSkeleton()
        {
            // Initialize Form
            // InitializeComponent();
        }

        public void FormLoad()
        {
            // Initialize comm
            comm = new Communication(serverIP, serverPort, LogMessage);
            comm.ClientLoad();
        }

        private void LogMessage(string message)
        {
            // No log message

            try
            {
                // ------------ All received logic here -------------
                Console.WriteLine("Server code received: " + message);
                // If message is not valid JSON, return
                if (!message.Contains("TableCode") || !message.Contains("type") || !message.Contains("from") || !message.Contains("to") || !message.Contains("message") || !message.Contains("date"))
                {
                    return;
                }

                // Extract message from JSON
                ChessAI.ChatServerAndClient.Message msg = ChessAI.ChatServerAndClient.Message.FromJson(message);
                if (msg != null)
                {
                    // Do something with the message
                    // Get tableCode: msg.TableCode
                    // Get type: msg.type
                    // Get from: msg.from
                    // Get to: msg.to
                    // Get message: msg.message
                    // Get date: msg.date

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in LogMessage: " + e.Message);
            }
        }

        public void SendMessage(string chatmessage)
        {
            // Send message: {"TableCode": newTableCode, "type": "chat", "from": userName, "to": opponentUserName, "message": message, "date": DateTime.Now}
            ChessAI.ChatServerAndClient.Message msg = new ChessAI.ChatServerAndClient.Message("123456", "chat", "testUser", "testUser", chatmessage, DateTime.Now);
            comm.SendMessage(msg.ToJson());
        }

        public void FormClosed()
        {
            // Close comm
            comm.ClientClose();
        }

    }
}