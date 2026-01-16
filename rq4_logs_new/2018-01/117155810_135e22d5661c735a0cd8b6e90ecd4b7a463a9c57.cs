using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SharpNetwork
{
    public class NetworkServer
    {
        private readonly string _ipAddress;
        private readonly int _port;

        private readonly TcpListener _tcpListener;

        private byte[] _readBuffer;

        public NetworkServer(string ipAddress, int port, int bufferSize = 4096)
        {
            // -- Define private variables
            _ipAddress = ipAddress;
            _port = port;

            // -- Init serverClients list
            _serverClients = new SortedList<Guid, ServerClient>();

            // -- Set up tcp listener
            _tcpListener = new TcpListener(IPAddress.Parse(_ipAddress), _port);

            // -- Start listening
            _tcpListener.Start();

            // -- Begin accepting clients Async
            _tcpListener.BeginAcceptTcpClient(HandleAcceptClient, _tcpListener);
        }

        private readonly SortedList<Guid, ServerClient> _serverClients;

        /**
         * Delegate callback when tcp client is accepted.
         */
        private void HandleAcceptClient(IAsyncResult result)
        {
            // -- Get tcp client
            TcpClient newTcpClient = _tcpListener.EndAcceptTcpClient(result);

            // -- Create new client
            Guid newServerClientGuid = new Guid();
            ServerClient newServerClient = new ServerClient(newTcpClient, newServerClientGuid);

            // -- Add client to list
            _serverClients.Add(newServerClientGuid, newServerClient);

            // -- Call OnClientConnected
            OnClientConnected(newServerClient);
        }

        protected virtual void OnClientConnected(ServerClient serverClient)
        {
            serverClient.SendMessage("Welcome...");
        }

        /**
         * Closes the tcp listener.
         */
        public void Close()
        {
            _tcpListener.Stop();
        }
    }
}