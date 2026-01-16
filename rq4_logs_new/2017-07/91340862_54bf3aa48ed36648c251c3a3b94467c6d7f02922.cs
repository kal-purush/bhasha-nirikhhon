using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace MainForm
{
    public partial class MainForm : Form
    {

        private static readonly Socket ClientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        private void ConnectToServer(IPAddress host, int port)
        {
            int attempts = 0;

            while (!ClientSocket.Connected)
            {

                try
                {
                    attempts++;
                    AppendClientStatus("Connection attempt " + attempts);
                    // Change IPAddress.Loopback to a remote IP to connect to a remote host.

                    ClientSocket.Connect(host, port);
                }
                catch (SocketException)
                {
                    AppendClientStatus("SocketException");
                }
            }
            AppendClientStatus("Connected");
        }

        /// <summary>
        /// Close socket and exit program.
        /// </summary>
        private void Exit()
        {
            SendString("exit"); // Tell the server we are exiting
            ClientSocket.Shutdown(SocketShutdown.Both);
            ClientSocket.Close();
            AppendClientStatus("Client disconnected");
        }

        private void SendRequest()
        {
            string request = txt_message_client.Text;
            SendString(request);
            //txt_status_client.Invoke((MethodInvoker)delegate
            //{

            AppendClientStatus("request sent : " + request);
                if (request.ToLower() == "exit")
                {
                    Exit();
                }
            //});

        }

        /// <summary>
        /// Sends a string to the server with ASCII encoding.
        /// </summary>
        private void SendString(string text)
        {
            byte[] buffer = Encoding.ASCII.GetBytes(text);
            ClientSocket.Send(buffer, 0, buffer.Length, SocketFlags.None);
        }

        private void ReceiveResponse()
        {
            //txt_status_client.Invoke((MethodInvoker)delegate
            //{
                var buffer = new byte[2048];
                int received = ClientSocket.Receive(buffer, SocketFlags.None);
                if (received == 0) return;
                var data = new byte[received];
                Array.Copy(buffer, data, received);
                string text = Encoding.ASCII.GetString(data);
                AppendClientStatus("Response : " + text);
            //});
        }


    }
}