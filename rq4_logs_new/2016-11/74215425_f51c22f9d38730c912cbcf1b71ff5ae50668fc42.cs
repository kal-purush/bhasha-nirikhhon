/*
 * Author: Gence Özer 
 * Date: 11/19/2016
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

namespace server
{
    public partial class serverForm : Form
    {
        Socket socket;
        EndPoint epLocal;
        List<string> connectedUsers = new List<String>();
        Thread dispatcherThread;

        public serverForm()
        {
            InitializeComponent();
        }

        /*
         * This function prints the given message in the 
         * Logger rich text box in the GUI
         */
        private void printLogger(string message)
        {
            string now = DateTime.Now.ToString(@"MM\/dd\/yyyy h\:mm tt");
            RTextBox_Logs.AppendText(now + "--> "+ message + "\n");
        }

         /*
          * This function returns local ip address of the server
          * If the server is not connected to internet, it returns 
          * local host ip.
          */
        private string getLocalIP()
        {
            IPHostEntry host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    printLogger("Server ip: " + ip.ToString());
                    return ip.ToString();
                }
            }
            printLogger("Server ip: 127.0.0.1 ");
            return "127.0.0.1";
        }

        /*
         * This function checks if the directory in the given 
         * path exists, if it doesn't it creates a directory
         */
        private void createDirectoryInPath(String path)
        {
            try
            {
                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                    printLogger("Creating the directory in the path: " + path);
                }
                else
                {
                    printLogger("Directory already exists in the path: " + path);
                }
            }
            catch (Exception exc)
            {
                MessageBox.Show("Exception occured on directory creation:" + exc.Message);
                return;
            }
        }

        /*
         * This function retrieves path and port information from GUI
         * First, it creates a directory, if not exists, 
         * then it binds a socket and puts it in a listening state to
         * given port info.
         */
        private void initalizeListening()
        {
            //Binding the socket to ip and given port name, puts the socket in listening state
            int portNumber =  (int)Numeric_Port.Value;
            epLocal = new IPEndPoint(IPAddress.Parse(getLocalIP()), portNumber);
            try
            {
                socket.Bind(epLocal);
                socket.Listen(portNumber);
            }catch(Exception exc)
            {
                MessageBox.Show("Exception occured: " + exc.Message);
            }
              
            // Sets up the thread which will perform the handshake connection with the client 
            try
            {
                dispatcherThread = new Thread(new ThreadStart(dispatchFileTransferOperations));
                dispatcherThread.Start();
                printLogger("Dispatcher thread starts working.");
                printLogger("Server is now listening on port: " + portNumber);
            }
            catch (Exception exc)
            {
                MessageBox.Show("Thread failed to start: " + exc.Message);
            } 
        }

        /*
         * This function continously listens to given port
         * when a connection comes, it transfers the connection
         * to a new socket that will be handled in a seperate thread.
         * After, thread runs the socket keeps on listening for 
         * new incoming connections.
         */
        private void dispatchFileTransferOperations() 
        {
            while (true)
            {
                try
                {
                    Socket dataTransferSocket = socket.Accept();
                    printLogger("A new incomming connection accepted");
                    Thread clientConnectionThread = new Thread(new ParameterizedThreadStart(handleUserConnection));
                    clientConnectionThread.Start(dataTransferSocket);
                }
                catch (Exception exc)
                {
                    MessageBox.Show("Exception occured while listening: " + exc.Message);
                }    
            }
        }

        /*
         * This function allows users to make multiple file
         * transfer throughout their connection
         * TODO:finish the function
         */
        private void handleUserConnection(Object socketObj)
        {
            Socket socket = (Socket)socketObj;
            //Recieves the username, filename and filesize to establish the connection
            byte[] handshakeInfo = new byte[128];
            try
            {
                int recievedData = socket.Receive(handshakeInfo);
                //If 0 bytes are recieved connection is dropped
                if (recievedData == 0)
                {
                    MessageBox.Show("Client disconnected");
                    return;
                }
            }
            catch (Exception exc)
            {
                MessageBox.Show("Socket exception occured: " + exc.Message);
            }
           
            ASCIIEncoding encoder = new ASCIIEncoding();
            //Parsing username 
            int usernameSize = BitConverter.ToInt32(handshakeInfo.Take(4).ToArray(), 0);
            string username = encoder.GetString(handshakeInfo.Skip(4).Take(usernameSize).ToArray());

            printLogger("An incoming connection request from user: " + username);

            if (checkUserList(username))
            {
                //Username already has a active connection, terminate
                printLogger("user " + username + "already has an active connection. Terminating connection.");
                termianteUserConnection(socket, username);
                return;
            }

            //Add the username in the list
            connectedUsers.Add(username);
            sendResultToClient(socket,0);

            printLogger("Checking for existing directory of user: " + username);
            createDirectoryInPath(serverPath.Text + "\\" + username);

            bool isClientConnected = true;
            while (isClientConnected)
            {
                isClientConnected = transferData(socketObj,username);
            }
        }

        /*
         * This function takes a socket object as a parameter. 
         * First, it recieves the handshake information from the 
         * client, which contains username size,username, filename size,
         * filename, filesize. Then, it verifies that this is a unique client
         * by checking it on the namelist. If it verifies, it initiates data transfer
         * over TCP sockets.
         */
        private bool transferData(Object socketObj, string username)
        {
            Socket socket = (Socket)socketObj;
             
            byte[] fileInfo = new byte[128];
            try
            {
                 int recievedData = socket.Receive(fileInfo);
                //If 0 bytes recieved socket connection is lost
                 if (recievedData == 0)
                 {
                     MessageBox.Show("Client disconnected");
                     termianteUserConnection(socket, username);
                     return false;
                 }
            }
            catch (Exception exc)
            {
                MessageBox.Show("Socket exception occured: " + exc.Message);
            }
           
            ASCIIEncoding encoder = new ASCIIEncoding();
            //Parsing filename and fileSize
            int filenameSize = BitConverter.ToInt32(fileInfo.Take(4).ToArray(), 0);
            string filename = encoder.GetString(fileInfo.Skip(4).Take(filenameSize).ToArray());
            long fileSize = BitConverter.ToInt64(fileInfo.Skip(4 + filenameSize).Take(sizeof(long)).ToArray(), 0);

            printLogger("File transfer started for user:" + username + " filesize: " + fileSize);
            
            /*
             * Recieves the file in chunks of 2KB, it continues to recieve until 
             * the file transfer is completed.
             */ 
            byte[] data = new byte[8 * 1024];
            FileStream stream = File.Create(serverPath.Text + "\\" + username + "\\" + filename);
            try
            {
                long bytesLeftToTransfer = fileSize;
                printLogger("Filesize: " + fileSize);

                while (bytesLeftToTransfer > 0)
                {
                    long amountOfBytes = socket.Receive(data);
                    //If 0 bytes is recieved socket connection is dropped
                    if (amountOfBytes == 0)
                    {
                        MessageBox.Show("Client disconnected");
                        throw new SocketException();
                    }
                    long bytesToCopy = Math.Min(amountOfBytes, bytesLeftToTransfer);
                    stream.Write(data, 0, (int)bytesToCopy);
                    bytesLeftToTransfer -= bytesToCopy;
                }
                printLogger("Stream closed for " + username);
                stream.Close();
            }
            catch (SocketException exc)
            {
                MessageBox.Show("Socket Exception occured");
                stream.Close();
                File.Delete(serverPath.Text + "\\" + username + "\\" + filename);
                printLogger("Corruped filed deleting ");
                termianteUserConnection(socket, username);
                return false;
            }
            catch (Exception exc)
            {
                MessageBox.Show(exc.Message);
                termianteUserConnection(socket, username);
                return false;
            }
            printLogger("File transfer finished for user:" + username);
            return true;
         }
        

        /*
         *  This method terminates the sockets 
         */
        private void terminateSocket(Socket sck)
        {
            sck.Shutdown(SocketShutdown.Both);
            sck.Close();
        }

        private void Button_Start_Click(object sender, EventArgs e)
        {
            initalizeListening();
            Button_Start.Enabled = false;
            //Button_Stop.Enabled = true;
        }

        private void serverForm_Load(object sender, EventArgs e)
        {
            TextBox.CheckForIllegalCrossThreadCalls = false;
            Button_Stop.Enabled = false;

            //Creating a socket to listen the incoming requests
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        }

        private void Button_Stop_Click(object sender, EventArgs e)
        {
            terminateSocket(socket);
            Button_Stop.Enabled = false;
            Button_Start.Enabled = true;
        }

        /*
         * This method iterates over the user list to find out 
         * whether the username already exists on the list
         */
        private bool checkUserList(string nameToCheck)
        {
            foreach(string clientName in connectedUsers)
            {
                if (clientName.Equals(nameToCheck)) 
                {
                    // User name exists in the list return true
                    return true;
                }
            }
            // Iterated over all of the list, username is not in the list
            return false;
        }

        private void removeFromUserList(string username)
        {
            connectedUsers.Remove(username);
        }

        private void termianteUserConnection(Socket socket, string username)
        {
            sendResultToClient(socket,1);
            terminateSocket(socket);
            removeFromUserList(username);
        }

        private void sendResultToClient(Socket socket, Int32 result)
        {
            socket.Send(BitConverter.GetBytes(result));
        }

        private void serverBrowse_Click(object sender, EventArgs e)
        {

            string folderPath = "";
            FolderBrowserDialog folderBrowserDialog1 = new FolderBrowserDialog();
            if (folderBrowserDialog1.ShowDialog() == DialogResult.OK)
            {
                folderPath = folderBrowserDialog1.SelectedPath;
                serverPath.Text = folderPath;
                printLogger("You chose the path " + folderPath + " to upload items.");
            }
        }
    }
}