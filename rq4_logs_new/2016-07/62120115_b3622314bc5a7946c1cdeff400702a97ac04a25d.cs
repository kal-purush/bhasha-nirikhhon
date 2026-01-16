using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Net;
using System.Windows;

namespace easyBJUT
{
    class Client
    {
        // Status Key
        private const byte CHECK_ROOM_LIST = 0;
        private const byte REQUEST_ROOM_MSG = 1;
        private const byte SEND_MSG = 2;
        private const byte DISCONNECT = 3;
        private const byte IS_RECEIVE_MSG = 4;
        private const byte IS_NOT_RECEIVE_MSG = 5;
        private const byte INVALID_MESSAGE = 6;

        private const string ipAddr = "127.0.0.1";  // watching IP
        private const int port = 3000;              // watching port

        // client thread, used for receive message
        private Thread threadClient = null;
        // client socket, used for connect server
        private Socket socketClient = null;

        #region --- Connect To The Server ---
        /// <summary>
        ///     Connect to the server
        /// </summary>
        public Client()
        {
            // get IP address
            IPAddress address = IPAddress.Parse(ipAddr);
            // create the endpoint
            IPEndPoint endpoint = new IPEndPoint(address, port);
            // create the socket, use IPv4, stream connection and TCP protocol
            socketClient = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                // Connect to the Server
                socketClient.Connect(endpoint);

                threadClient = new Thread(ReceiveMsg);
                threadClient.IsBackground = true;
                threadClient.Start();
            }
            catch (SocketException se)
            {
                MessageBox.Show("[SocketError]Connection failed: " + se.Message);
            }
            catch (Exception ex)
            {
                MessageBox.Show("[Error]Connection failed: " + ex.Message);
            }
        }
        #endregion

        #region --- Check Room List ---
        /// <summary>
        ///     check room list
        /// </summary>
        /// <param name="roomList"></param>
        public void CheckRoomList(List<string> roomList)
        {
            string s_roomList = JsonConvert.SerializeObject(roomList);

            SendMsg(CHECK_ROOM_LIST, s_roomList);
        }
        #endregion

        #region --- Add New Message ---
        /// <summary>
        ///     add new message
        /// </summary>
        /// <param name="roomId"></param>
        /// <param name="msg"></param>
        public void AddNewMsg(string roomId, string msg)
        {
            MsgHandler msgHandler = new MsgHandler(roomId, msg);
            string sendMsg = JsonConvert.SerializeObject(msgHandler);
            
            SendMsg(SEND_MSG, sendMsg);
        }
        #endregion

        #region --- Off Line ---
        /// <summary>
        ///     go off line
        /// </summary>
        public void GoOffLine()
        {
            SendMsg(DISCONNECT, "");
        }
        #endregion

        #region --- Send Message ---
        /// <summary>
        ///     Send Message
        /// </summary>
        /// <param name="flag">msg type</param>
        /// <param name="msg">message</param>
        private void SendMsg(byte flag, string msg)
        {
            try
            {
                byte[] arrMsg = Encoding.UTF8.GetBytes(msg);
                byte[] sendArrMsg = new byte[arrMsg.Length + 1];

                // set the msg type
                sendArrMsg[0] = flag;
                Buffer.BlockCopy(arrMsg, 0, sendArrMsg, 1, arrMsg.Length);

                socketClient.Send(sendArrMsg);
            }
            catch (SocketException se)
            {
                Console.WriteLine("[SocketError] send message error : {0}", se.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("[Error] send message error : {0}", e.Message);
            }
        }
        #endregion

        #region --- Receive Message ---
        /// <summary>
        ///     receive message
        /// </summary>
        private void ReceiveMsg()
        {
            while (true)
            {
                // define a buffer for received message
                byte[] arrMsg = new byte[1024 * 1024 * 2];

                // length of message received
                int length = -1;

                try
                {
                    // get the message
                    length = socketClient.Receive(arrMsg);

                    // encoding the message
                    string msgReceive = Encoding.UTF8.GetString(arrMsg, 1, length-1);

                    if (arrMsg[0] == SEND_MSG)
                    {

                    }
                    else if (arrMsg[0] == IS_RECEIVE_MSG)
                    {

                    }
                    else if (arrMsg[0] == IS_NOT_RECEIVE_MSG)
                    {

                    }
                    else if (arrMsg[0] == INVALID_MESSAGE)
                    {

                    }
                    else
                    {

                    }

                }
                catch (SocketException se)
                {
                    //MessageBox.Show("【错误】接收消息异常：" + se.Message);
                    return;
                }
                catch (Exception e)
                {
                    //MessageBox.Show("【错误】接收消息异常：" + e.Message);
                    return;
                }
            }
        }
        #endregion
    }

    public struct MsgHandler
    {
        public string roomId;
        public string msg;

        public MsgHandler(string r, string m)
        {
            roomId = r; msg = m;
        }
    }
}