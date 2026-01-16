/*
 * Author: Eylül Dicle Yurdakul
 * Date: 12/19/2016
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace myClient
{
    public partial class formClient : Form
    {
        Socket cliSocket;
        byte[] receivedBytes = new byte[2048];
        byte[] fileData;

        public formClient()
        {
            InitializeComponent();
            TextBox.CheckForIllegalCrossThreadCalls = false;
        }

        private void printLogger(string message)
        {
            string now = DateTime.Now.ToString(@"MM\/dd\/yyyy h\:mm tt");
            clientBox.AppendText(now + "--> " + message + "\n");
        }

        private void clientConnect_Click(object sender, EventArgs e)
        {
            cliSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            cliSocket.Connect(clientIP.Text, Convert.ToInt32(clientPort.Text)); 
        }

        private void clientBrowse_Click(object sender, EventArgs e)
        {
            printLogger("You are connected to the system as " + clientUsername.Text + ".");
            OpenFileDialog fDialog = new OpenFileDialog();
            if (fDialog.ShowDialog() == DialogResult.OK)
            {
                MessageBox.Show(fDialog.FileName.ToString());
                clientText.Text = fDialog.FileName.ToString();
                printLogger("You chose " + Path.GetFileName(fDialog.FileName) + " to upload.");
            }
            fDialog.AddExtension = true;
            fDialog.CheckFileExists = true;
            fDialog.CheckPathExists = true;  
        }

        private void clientSend_Click(object sender, EventArgs e)
        {
            ASCIIEncoding aEncoder = new ASCIIEncoding();
            printLogger("System is uploading the file/folder... ");
            long filesize = new System.IO.FileInfo(clientText.Text).Length;
            string filename = Path.GetFileName(clientText.Text);

            byte[] filenameInBytes = aEncoder.GetBytes(filename);
            byte[] fileInfo = new byte[12 + filename.Length];

            MessageBox.Show(filesize.ToString() + " " + filenameInBytes.Length);
            //Filling fileInfo byte array
            Buffer.BlockCopy(BitConverter.GetBytes(filenameInBytes.Length), 0, fileInfo, 0, 4);
            Buffer.BlockCopy(filenameInBytes, 0, fileInfo, 4, filenameInBytes.Length);
            Buffer.BlockCopy(BitConverter.GetBytes(filesize), 0, fileInfo, 4 + filenameInBytes.Length, sizeof(long));

            try
            {
                cliSocket.Send(fileInfo);
            }
            catch (Exception exc)
            {
                MessageBox.Show("Exception occured during data transfer: " + exc.Message);
                return;
            }

            try
            {
               cliSocket.SendFile(clientText.Text);
            }
            catch (Exception exc)
            {
                MessageBox.Show("Exception occured during data transfer: " + exc.Message);
                return;
            }
        }

        private void formClient_Load(object sender, EventArgs e)
        {
            clientConnect.Enabled = false;
        }

        private void clientConnect_Click_1(object sender, EventArgs e)
        {
            if (clientConnect.Text == "Connect")   
            {
                try
                {
                    cliSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    cliSocket.Connect(clientIP.Text, Convert.ToInt32(clientPort.Text));

                    ASCIIEncoding aEncoder = new ASCIIEncoding();
                    string username = clientUsername.Text;
                    byte[] usernameInBytes = aEncoder.GetBytes(username);
                    byte[] usernameInfo = new byte[4 + username.Length];

                    //Filling usernameData byte array
                    Buffer.BlockCopy(BitConverter.GetBytes(usernameInBytes.Length), 0, usernameInfo, 0, 4);
                    Buffer.BlockCopy(usernameInBytes, 0, usernameInfo, 4, usernameInBytes.Length);

                    try
                    {
                        cliSocket.Send(usernameInfo);
                        byte[] result = new byte[4];
                        cliSocket.Receive(result);
                        if(BitConverter.ToInt32(result,0).Equals(1))
                        {
                            MessageBox.Show("Connection refused. Your username is not unique");
                            cliSocket.Shutdown(SocketShutdown.Both);
                            cliSocket.Close();
                            return;
                        }

                    }
                    catch (Exception exc)
                    {
                        MessageBox.Show("Exception occured during data transfer: " + exc.Message);
                        return;
                    }
                    clientConnect.Text = "Disconnect!";
                    clientConnect.BackColor = Color.Orange;
                    clientSend.Enabled = true;
                }
                catch
                {
                    MessageBox.Show("ERROR: UNABLE TO CONNECT");
                }
            }
            else      // For disconnection   
            {
                clientConnect.Text = "Connect";
                clientConnect.BackColor = DefaultBackColor;
                clientSend.Enabled = false;
                cliSocket.Shutdown(SocketShutdown.Both);
                cliSocket.Close();
            }
        }
        //  Enable Connect button if the username box is not empty
        private void clientUsername_TextChanged(object sender, EventArgs e)  
        {
            clientConnect.Enabled = !string.IsNullOrEmpty(clientUsername.Text);
        }
    }
}