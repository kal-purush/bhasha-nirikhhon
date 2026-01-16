using ChessAI.ChatServerAndClient;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace winforms_chat
{
    public partial class ChatServerCode : Form
    {

        // Variables
        private Communication comm;
        string serverIP = "127.0.0.1";
        int serverPort = 9999;
        public ChatServerCode()
        {
            InitializeComponent();
        }

        private void ClientForm_Load(object sender, EventArgs e)
        {
            comm = new Communication(serverIP, serverPort, LogMessage);
            comm.ClientLoad();
        }

        private void LogMessage(string message)
        {
            // No log message

            try
            {
                if (InvokeRequired)
                {
                    Invoke(new Action<string>(LogMessage), message);
                    return;
                }
                // ------------ All received logic here -------------
                Console.WriteLine("Received: " + message);
                // If message is not valid JSON, return
                if (!message.Contains("TableCode") || !message.Contains("type") || !message.Contains("from") || !message.Contains("to") || !message.Contains("message") || !message.Contains("date"))
                {
                    return;
                }

                // Extract message from JSON
                ChessAI.ChatServerAndClient.Message msg = ChessAI.ChatServerAndClient.Message.FromJson(message);
                if (msg != null)
                {
                    
                }

            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message, "Warning");
            }

        }

        private void ClientForm_FormClosed(object sender, FormClosedEventArgs e)
        {
            // Send last message to server
            // TableCode: 000000
            // type: join
            // from: txt_userName.Text
            // to: server
            // message: disconnected
            // date: DateTime.Now
            //ChessAI.ChatServerAndClient.Message message = new ChessAI.ChatServerAndClient.Message("000000", "join", txt_userName.Text, "server", "disconnected", DateTime.Now);
            //SendMessage(message.ToJson());
            //// Close the stream
            //if (stream != null)
            //{
            //    stream.Close();
            //}
            //// Close the client
            //if (client != null)
            //{
            //    client.Close();
            //}
        }
    }
}

