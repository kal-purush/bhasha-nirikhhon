using System;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Entities;
using Server.Details.Ports;

namespace Server.Adapters
{
    internal class SocketListener : IListener<IDevice>
    {
        private readonly int _port;
        private readonly EventWaitHandle _accepted;
        public event EventHandler<EventArgs> OnShutdown;
        public Port<IDevice> OutPort { get; }

        public SocketListener()
            : this(10863)
        {
        }

        public SocketListener(int port)
        {
            _port = port;
            _accepted = new ManualResetEvent(false);
            OutPort = new SimplePort<IDevice>();
        }

        public void Listen()
        {
            var ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
            //var localEndPoint = new IPEndPoint(ipHostInfo.AddressList[0], _port);
            var localEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), _port);

            Logger.WriteInfo($"Local address and port: {localEndPoint}");

            var listener = new Socket(
                localEndPoint.Address.AddressFamily,
                SocketType.Stream,
                ProtocolType.Tcp
            );

            listener.Bind(localEndPoint);
            listener.Listen(100);

            while (true)
            {
                _accepted.Reset();

                Logger.WriteInfo("Waiting for a connection...");
                listener.BeginAccept(AcceptCallback, listener);
                _accepted.WaitOne();
            }
        }

        private void AcceptCallback(IAsyncResult ar)
        {
            _accepted.Set();
            var listener = (Socket) ar.AsyncState;
            var handler = listener.EndAccept(ar);

            var worker = new Worker
            {
                Socket = handler
            };

            handler.BeginReceive(
                worker.Buffer,
                0,
                worker.BufferSize,
                SocketFlags.None,
                ReadCallback,
                worker
            );
        }

        private void ReadCallback(IAsyncResult ar)
        {
            var worker = (Worker) ar.AsyncState;
            var handler = worker.Socket;

            var read = handler.EndReceive(ar);

            if (read > 0)
            {
                worker.Data.Append(Encoding.ASCII.GetString(worker.Buffer, 0, read));
                var content = worker.Data.ToString();
                if (content.EndsWith(Environment.NewLine))
                {
                    content = content.TrimEnd(Environment.NewLine.ToCharArray());
                    if (content == "quit")
                    {
                        worker.Socket.Shutdown(SocketShutdown.Both);
                        worker.Socket.Close();
                    }
                    else
                    {
                        TransferData(worker);
                    }
                }
                else
                {
                    handler.BeginReceive(
                        worker.Buffer,
                        0,
                        worker.BufferSize,
                        SocketFlags.None,
                        ReadCallback,
                        worker
                    );
                }
            }
        }

        private void TransferData(Worker worker)
        {
            var content = worker.Data.ToString().TrimEnd(Environment.NewLine.ToCharArray());
            Logger.WriteInfo($"Read {content.Length} bytes from socket. \n Data: {content}");

            OutPort.Transfer(new WorkerDevice(worker, this));

            worker.Socket.Shutdown(SocketShutdown.Both);
            worker.Socket.Close();
        }

        public void Send(Socket handler, string data)
        {
            var byteData = Encoding.ASCII.GetBytes(data);
            handler.BeginSend(
                byteData,
                0,
                byteData.Length,
                SocketFlags.None,
                SendCallback,
                handler);
        }

        private void SendCallback(IAsyncResult ar)
        {
            var handler = (Socket) ar.AsyncState;
            var bytesSent = handler.EndSend(ar);

            Logger.WriteInfo($"Sent {bytesSent} bytes to client.");
        }
    }

    internal class Worker
    {
        public Socket Socket { get; set; }
        public int BufferSize { get; }
        public byte[] Buffer { get; }
        public StringBuilder Data { get; }

        public Worker()
        {
            BufferSize = 1024;
            Buffer = new byte[BufferSize];
            Data = new StringBuilder();
        }
    }

    internal class WorkerDevice : IDevice
    {
        private readonly Worker _worker;
        private readonly SocketListener _listener;

        public WorkerDevice(Worker worker, SocketListener listener)
        {
            _worker = worker;
            _listener = listener;
        }

        public void Open()
        {
        }

        public string ReadLine()
        {
            return _worker.Data.ToString().TrimEnd(Environment.NewLine.ToCharArray());
        }

        public void Write(char[] s, int index, int count)
        {
            WriteLine(new string(s) + Environment.NewLine);
        }

        public void WriteLine(string s)
        {
            _listener.Send(_worker.Socket, s);
        }

        public void Seek(int pos)
        {
        }

        public void Close()
        {
        }
    }
}