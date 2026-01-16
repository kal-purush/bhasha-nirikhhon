using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Entities;

namespace Server.Adapters.Http
{
    internal class HttpListener : IListener
    {
        private readonly IPAddress _ipAddress;
        private readonly int _port;
        private readonly EventWaitHandle _stop;
        private Socket _serverSocket;

        public HttpListener(IPAddress ipAdress, int port)
        {
            _stop = new ManualResetEvent(false);
            _ipAddress = ipAdress;
            _port = port;
        }

        public void Listen()
        {
            _serverSocket = new Socket(
                _ipAddress.AddressFamily,
                SocketType.Stream,
                ProtocolType.Tcp
            );

            _serverSocket.Bind(new IPEndPoint(_ipAddress, _port));
            _serverSocket.Listen(100);
            Logger.WriteInfo($"Http Listener running on {_ipAddress}:{_port}");
        }

        public void Stop()
        {
            _stop.Set();
            try
            {
                _serverSocket.Shutdown(SocketShutdown.Both);
                _serverSocket.Close();
            }
            catch (SocketException e)
            {
                Logger.WriteError($"Exception while during httplistener shutdown: {e}");
                throw;
            }
        }

        public async Task<Socket> AcceptAsync()
        {
            var clientSocket = await _serverSocket.AcceptAsync();
            Logger.WriteInfo($"Connection accepted from {clientSocket.LocalEndPoint}");
            return clientSocket;
        }
    }
}