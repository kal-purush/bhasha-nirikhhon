using System;
using System.IO;
using System.Threading;
using System.Net.Sockets;
using System.Collections.Generic;

namespace Ezzen
{

    public class ClientSocket
    {
        private System.Net.Sockets.TcpClient clientSocket;
        private NetworkStream serverStream;
        private String clientID;
        private String cachePath;
        private Thread recvMessageThread;
        private readonly object syncLock;

        private Dictionary<String, GroupMessenger> groupChats;

        //getters & setters;
        public string ClientID { get => clientID;}

        public ClientSocket()
        {
            clientSocket = new System.Net.Sockets.TcpClient();
            serverStream = default(NetworkStream);
            clientID = null;
            cachePath = null;
            groupChats = new Dictionary<string, GroupMessenger>();
            recvMessageThread = new Thread(recvMessageAsyn);
            syncLock = new object();
            //connect(ipaddress);
        }

        public void connect(String ipaddress)
        {
            clientSocket.Connect(ipaddress, 8888);
            serverStream = clientSocket.GetStream();
            //recvMessageThread.Start();
        }

        public void disconnect()
        {
            recvMessageThread.Abort();
            String msg = "C" + Message.Separator + "DISC" + Message.Separator + clientID;
            sendMsg(msg);
            clientSocket.Close();
        }

        public String signup(String username, String password)
        {
            String msg = "C" + Message.Separator + "SIGN" + Message.Separator + username + Message.Separator + password;
            Console.WriteLine(msg);
            sendMsg(msg);

            msg = recvMessage();

            Console.WriteLine(msg);
            String[] proc_msg = Message.splitString(msg);
            if (proc_msg[0] == "R")
            {
                if (proc_msg[1] == "SUCC") return "SIGNUP SUCCEED";
                else if (proc_msg[1] == "FAIL") return "SIGNUP FAIL";
            }
            return "ERROR";
        }

        public String login(String username, String password)
        {
            String msg = "C" + Message.Separator + "LOGN" + Message.Separator + username + Message.Separator + password;
            sendMsg(msg);

            msg = recvMessage();
            String[] proc_msg = Message.splitString(msg);
            if (proc_msg[0] == "R")
            {
                if (proc_msg[1] == "SUCC")
                {
                    clientID = proc_msg[2];
                    cachePath = "localcache\\" + clientID + ".txt";
                    return clientID;
                }
                else if (proc_msg[1] == "FAIL") return "LOGIN FAIL";
            }
            return "ERROR";
        }

        public void joinGroup(String groupID)
        {
            lock (syncLock)
            {
                String msg = "C" + Message.Separator + "JOIN" + Message.Separator + clientID + Message.Separator + groupID;
                sendMsg(msg);
                groupChats.Add(groupID, new GroupMessenger(groupID, clientID));
                groupChats[groupID].startThread();
            }
            // Add group on screen
        }

        public void exitGroup(String groupID)
        {
            lock (syncLock)
            {
                String msg = "C" + Message.Separator + "EXTG" + Message.Separator + clientID + Message.Separator + groupID;
                sendMsg(msg);
                groupChats[groupID].abortThread();
                groupChats[groupID].saveCache();
                groupChats.Remove(groupID);
            }
            // Remove group on screen 
        }

        public void leaveGroup(String groupID)
        {
            lock (syncLock)
            {
                String msg = "C" + Message.Separator + "LEVG" + Message.Separator + clientID + Message.Separator + groupID;
                sendMsg(msg);
                groupChats[groupID].abortThread();
                groupChats.Remove(groupID);
            }
            // Remove group on screen and delete local cache
        }

        public void loadUnread(String groupID)
        {
            lock (syncLock)
            {
                String msg = "C" + Message.Separator + "LUNR" + Message.Separator + clientID + Message.Separator + groupID + Message.Separator + groupChats[groupID].getLastMessageNo().ToString();
                sendMsg(msg);
                do
                {
                    msg = recvMessage();
                    String[] proc_msg = Message.splitString(msg);
                    if (proc_msg[0] == "R" && proc_msg[1] == "FINS") break;
                    if (proc_msg[0] == "M")
                    {
                        groupChats[groupID].feedMessage(new Message(Convert.ToUInt32(proc_msg[1]), proc_msg[2], proc_msg[3], Convert.ToDateTime(proc_msg[4])));
                    }
                } while (true);
            }
        }

        private void sendMsg(String msg)
        {
            byte[] outStream = System.Text.Encoding.ASCII.GetBytes(msg);
            serverStream.Write(outStream, 0, outStream.Length);
            serverStream.Flush();
        }

        private String recvMessage()
        {
            byte[] inStream = new byte[512];
            serverStream.Read(inStream, 0, 512);
            //Console.WriteLine(inStream);
            return System.Text.Encoding.ASCII.GetString(inStream).Trim();
        }

        private void recvMessageAsyn()
        {
            while (true)
            {
                byte[] inStream = new byte[512];
                serverStream.Read(inStream, 0, 512);
                String msg = System.Text.Encoding.ASCII.GetString(inStream).Trim();
                String[] proc_msg = Message.splitString(msg);
                if (proc_msg[0] == "M")
                {
                    Message tmp = new Message(Convert.ToUInt32(proc_msg[1]), proc_msg[2], proc_msg[4], Convert.ToDateTime(proc_msg[5]));
                    lock (syncLock)
                    {
                        if (groupChats.ContainsKey(proc_msg[2])) groupChats[proc_msg[2]].feedBuffer(tmp);
                    }
                }
            }
        }

        private void loadCache()
        {
            using (StreamReader sr = File.OpenText(cachePath))
            {
                String msg;
                while ((msg = sr.ReadLine()) != null)
                {
                    GroupMessenger tmp = new GroupMessenger(msg, clientID);
                    tmp.loadCache();
                    groupChats.Add(msg, tmp);
                }
            }
        }

        private void saveCache()
        {
            using (StreamWriter sw = File.CreateText(cachePath))
            {
                foreach (var pair in groupChats)
                {
                    sw.WriteLine(pair.Key);
                    pair.Value.saveCache();
                }
            }
        }
    }
}