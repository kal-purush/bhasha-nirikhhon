using System;
using System.IO;
using System.Threading;
using System.Net.Sockets;

namespace ChatApplication.Network
{
    public class FileTransfer
    {
        private bool SENDER_OR_RECEIVER;
        //True for sender, false for receiver
        private short numberOfTransfersThreadsRemaining;
        private long acknowledgedTransfers;
        private object syncObject;

        int port1, port2;

        private Socket controlSocket, dataSocket;


        public FileTransfer(bool senderOrReceiver, string filePath = null, int intialPort = 0)
        {
            acknowledgedTransfers = 0;
            syncObject = new object();
            SENDER_OR_RECEIVER = senderOrReceiver;
            numberOfTransfersThreadsRemaining = (short)(SENDER_OR_RECEIVER ? 0 : 3);

            controlSocket = null;
            dataSocket = null;

            if (senderOrReceiver) {
                port1 = intialPort;

                dataSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                dataSocket.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Any, port1));
                dataSocket.Listen(1);
                dataSocket.BeginAccept(new AsyncCallback(EndAcceptDataConnection), filePath);
            }
        }

        private const int FILE_BUFFER_SIZE = 8175;

        static internal byte[] packFileMetadata(string filePath)
        {
            string fileName = Path.GetFileName(filePath);
            long length = (new FileStream(filePath, FileMode.Open)).Length;
            byte[] arr1 = System.Text.Encoding.ASCII.GetBytes(fileName);
            byte[] arr2 = BitConverter.GetBytes(System.Net.IPAddress.HostToNetworkOrder(length));
            int x = arr2.Length;
            byte[] arr = new byte[arr1.Length + arr2.Length];
            Buffer.BlockCopy(arr1, 0, arr, 0, arr1.Length);
            Buffer.BlockCopy(arr2, 0, arr, arr1.Length, arr2.Length);
            return arr;
        }

        static internal void unpackFileMetadata(byte[] buffer, out string filename, out long size)
        {
            byte[] _temp = new byte[8];
            Buffer.BlockCopy(buffer, buffer.Length - 8, _temp, 0, 8);
            size = System.Net.IPAddress.NetworkToHostOrder(BitConverter.ToInt64(_temp, 0));
            _temp = new byte[buffer.Length - 8];
            Buffer.BlockCopy(buffer, 0, _temp, 0, _temp.Length);
            filename = System.Text.Encoding.UTF8.GetString(_temp);
        }

        internal void managedSendFileOverSockets(Socket dataSocket, Socket controlSocket, string filePath)
        {
            short _count = 0;
            const short _INITIAL = 5000;

            Thread _thread = new Thread(() => monitorControlChannel(controlSocket, dataSocket));
            _thread.Name = "Control Socket monitor for " + dataSocket.RemoteEndPoint.ToString();
            _thread.IsBackground = true;
            _thread.Start();

            using (FileStream _fs = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)) {
                byte[] _fileBuffer = new byte[FILE_BUFFER_SIZE];

                long _size = _fs.Length;

                try {
                    long _index = 0;

                    //Send the initial chunk of data
                    while (dataSocket.Connected && controlSocket.Connected && _index < _size) {
                        _fs.Position = _index;
                        int _read = _fs.Read(_fileBuffer, 0, _fileBuffer.Length);

                        try {
                            NetworkCommunicationManagers.SendIntOverSocket(dataSocket, _read);

                            if (_read != _fileBuffer.Length) {
                                byte[] _temp = new byte[_read];
                                Buffer.BlockCopy(_fileBuffer, 0, _temp, 0, _read);
                                NetworkCommunicationManagers.SendByteArrayOverSocket(dataSocket, _temp);
                            }
                            else {
                                NetworkCommunicationManagers.SendByteArrayOverSocket(dataSocket, _fileBuffer);
                            }
                        }
                        catch (SocketException) {
                            continue;
                        }

                        _index += _read;
                        _count++;
                        if (_count == _INITIAL) {
                            _count = 0;
                            break;
                        }
                    }

                    //Send the remaining data
                    while (dataSocket.Connected && controlSocket.Connected && _index < _size) {

                        _fs.Position = _index;
                        int _read = _fs.Read(_fileBuffer, 0, _fileBuffer.Length);

                        try {
                            NetworkCommunicationManagers.SendIntOverSocket(dataSocket, _read);
                            if (_read != _fileBuffer.Length) {
                                byte[] _temp = new byte[_read];
                                Buffer.BlockCopy(_fileBuffer, 0, _temp, 0, _read);
                                NetworkCommunicationManagers.SendByteArrayOverSocket(dataSocket, _temp);
                            }
                            else {
                                NetworkCommunicationManagers.SendByteArrayOverSocket(dataSocket, _fileBuffer);
                            }
                        }
                        catch (SocketException) {
                            continue;
                        }

                        _index += _read;
                        _count++;
                        if (_count % 200 == 0) {
                            bool _wait = true;
                            while (true) {
                                lock (syncObject) {
                                    if (acknowledgedTransfers > 0) {
                                        acknowledgedTransfers--;
                                        _wait = false;
                                        break;
                                    }
                                }
                                if (_wait) {
                                    Thread.Sleep(100);
                                }
                            }
                        }

                    }
                }
                catch {

                }
            }
        }

        internal void monitorControlChannel(Socket controlSocket, Socket dataSocket)
        {
            try {
                while (controlSocket.Connected) {
                    byte[] arr;
                    NetworkCommunicationManagers.ReceiveByteArrayOverSocket(controlSocket, out arr, 1);
                    if (arr[0] == 255) {
                        NetworkCommunicationManagers.Disconnect(controlSocket);
                        NetworkCommunicationManagers.Disconnect(dataSocket);
                        break;
                    }
                    else {
                        lock (syncObject) {
                            acknowledgedTransfers++;
                        }
                    }
                }
            }
            finally {
                NetworkCommunicationManagers.Disconnect(controlSocket);
                NetworkCommunicationManagers.Disconnect(dataSocket);
            }
        }

        internal bool managedReceiveFileOverSockets(Socket dataSocket, Socket controlSocket, string filePath, long length)
        {
            short _count = 0;
            long _index;
            int _size;

            using (FileStream _fs = new FileStream(filePath, FileMode.CreateNew)) {
                _fs.SetLength(length);
                _index = 0;
                _size = 1;
                byte[] _buffer;
                while (dataSocket.Connected && controlSocket.Connected && _index < length) {
                    try {
                        if (!NetworkCommunicationManagers.ReceiveIntOverSocket(dataSocket, out _size)) {
                            continue;
                        };
                        _buffer = new byte[_size];
                        if (!NetworkCommunicationManagers.ReceiveByteArrayOverSocket(dataSocket, out _buffer, _size)) {
                            continue;
                        };
                    }
                    catch (SocketException) {
                        continue;
                    }

                    _fs.Position = _index;
                    _fs.Write(_buffer, 0, _buffer.Length);
                    _index += _size;
                    _count++;
                    if (_count % 200 == 0) {
                        _count = 0;
                        ThreadPool.QueueUserWorkItem(state => NetworkCommunicationManagers.SendByteArrayOverSocket(controlSocket, new byte[] { (byte)(new Random()).Next(0, 255) }));
                    }
                }
            }
            NetworkCommunicationManagers.SendByteArrayOverSocket(controlSocket, new byte[] { 255 });
            if (_index < _size) {
                File.Delete(filePath);
                return false;
            }
            return true;
        }

        internal void EndAcceptDataConnection(IAsyncResult ar)
        {
            try {
                port2 = NetworkCommunicationManagers.FindNextFreeTcpPort();
                Socket _clientSocket = dataSocket.EndAccept(ar);
                NetworkCommunicationManagers.Disconnect(dataSocket);
                dataSocket = _clientSocket;
                NetworkCommunicationManagers.SendIntOverSocket(_clientSocket, port2);

                controlSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                controlSocket.Bind(new System.Net.IPEndPoint(System.Net.IPAddress.Any, port2));
                controlSocket.Listen(1);
                controlSocket.BeginAccept(new AsyncCallback(EndAcceptControlConnection), ar.AsyncState);               
            }
            catch (Exception) {
                return;
            }
        }

        internal void EndAcceptControlConnection(IAsyncResult ar)
        {
            try {
                string _filePath = (string)ar.AsyncState;
                Socket _clientSocket = controlSocket.EndAccept(ar);
                NetworkCommunicationManagers.Disconnect(controlSocket);
                controlSocket = _clientSocket;
                byte[] _buffer = packFileMetadata(_filePath);
                NetworkCommunicationManagers.SendIntOverSocket(dataSocket, _buffer.Length);
                NetworkCommunicationManagers.SendByteArrayOverSocket(dataSocket, _buffer);

                managedSendFileOverSockets(dataSocket, controlSocket, _filePath);
            }
            catch (Exception) {
                return;
            }
        }

        internal bool AcceptFileTransfer(Socket socket, int port1)
        {

            Socket _socket1, _socket2;
            int _port2;
            SocketException _socketException;

            if (!NetworkCommunicationManagers.ConnectToEndPoint(port1, socket.RemoteEndPoint.ToString().Remove(socket.RemoteEndPoint.ToString().LastIndexOf(':')), out _socket1, out _socketException)) {
                return false;
            }

            NetworkCommunicationManagers.ReceiveIntOverSocket(_socket1, out _port2);

            if (!NetworkCommunicationManagers.ConnectToEndPoint(_port2, socket.RemoteEndPoint.ToString().Remove(socket.RemoteEndPoint.ToString().LastIndexOf(':')), out _socket2, out _socketException)) {
                return false;
            }

            byte[] _buffer;
            int _temp;
            long _size;
            string _filename;

            NetworkCommunicationManagers.ReceiveIntOverSocket(_socket1, out _temp);
            NetworkCommunicationManagers.ReceiveByteArrayOverSocket(_socket1, out _buffer, _temp);
            unpackFileMetadata(_buffer, out _filename, out _size);

            if(!Directory.Exists(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Downloads", "ChatApp"))){
                Directory.CreateDirectory(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Downloads", "ChatApp"));
            };

            string _filePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Downloads", "ChatApp" , _filename);

            if (File.Exists(_filePath)) {
                string _extension, _assignedFilename;
                if (_filename.LastIndexOf('.') == -1) {
                    _extension = "";
                    _assignedFilename = _filename;
                }
                else {
                    _extension = "." + _filename.Remove(0, _filename.LastIndexOf('.') + 1);
                    _assignedFilename = _filename.Remove(_filename.Length - _extension.Length, _extension.Length);
                }
                
                for (int i = 0; true ; i++) {
                    _filePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Downloads", "ChatApp" , _assignedFilename + "(" + i + ")" + _extension);
                    if (!File.Exists(_filePath)) {
                        break;
                    }
                }
            }

            return managedReceiveFileOverSockets(_socket1, _socket2, _filePath, _size);
        }
    }
}