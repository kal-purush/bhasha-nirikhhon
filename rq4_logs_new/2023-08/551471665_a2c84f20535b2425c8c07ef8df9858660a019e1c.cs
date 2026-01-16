using LightBlueFox.Connect.Structure.ConnectionSources;
using LightBlueFox.Connect.Net;
using System.Net;
using System.Net.Sockets;

namespace LightBlueFox.Connect.Net.ConnectionSources
{
    public abstract class NetworkConnectionSource : ConnectionSource, IDisposable
    {
        

        public SocketProtocol Protocol { get; private set; }
        public int Port { get; private set; }
        public IPEndPoint LocalEndpoint { get; private set; }
        
        private Socket sock;

        public NetworkConnectionSource(Socket listener)
        {
            sock = listener;

            if (!sock.IsBound) throw new ArgumentException("Given socket was not yet bound.");
            if (sock.LocalEndPoint == null) throw new NullReferenceException("Local endpoint on socket is null!");
            
            Protocol = (SocketProtocol)sock.ProtocolType;            

            Port = ((IPEndPoint)sock.LocalEndPoint).Port;
            LocalEndpoint = (IPEndPoint)sock.LocalEndPoint;

            sock.Listen(((IPEndPoint)sock.LocalEndPoint).Port);
            sock.BeginAccept(new AsyncCallback(AcceptClient), null);
        }

        public NetworkConnectionSource(int port, SocketProtocol protocol, IPAddress? address = null)
        {
            if(protocol == SocketProtocol.UDP)
            {
                sock = new Socket(SocketType.Dgram, ProtocolType.Udp);
            }
            else if(protocol == SocketProtocol.TCP) 
            {
                sock = new Socket(SocketType.Stream, ProtocolType.Tcp);
            }
            else { throw new NotImplementedException("This source only supports TCP and UDP!"); }

            IPEndPoint ep = new IPEndPoint(address ?? IPAddress.Any, port);
            sock.Bind(ep);
            LocalEndpoint = ep;
            sock.Listen(port);
            sock.BeginAccept(new AsyncCallback(AcceptClient), null);
        }

        private void AcceptClient(IAsyncResult ar)
        {
            var client = sock.EndAccept(ar);
            Task.Run(() => { OnNewConnection?.Invoke(CreateConnection(client), this); });
            sock.BeginAccept(new AsyncCallback(AcceptClient), null);
        }

        protected abstract NetworkConnection CreateConnection(Socket s);

        public void Dispose()
        {
            ((IDisposable)sock).Dispose();
        }
    }
}