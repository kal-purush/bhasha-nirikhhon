using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Entities;

namespace Server.Adapters.Http
{
    internal class HttpConnection
    {
        private CancellationToken _token;
        private Socket _socketClient;
        private IHttpRouteTable _routeTable;

        public HttpConnection(Socket client, CancellationToken token, IHttpRouteTable routeTable)
        {
            _socketClient = client;
            _token = token;
            _routeTable = routeTable;
        }

        public Task Run()
        {
            return Task.Run(
                () =>
                {
                    const int arraySize = 2048;
                    var byteArray = new byte[arraySize];
                    var memoryStream = new MemoryStream(arraySize);
                    var netStream = new NetworkStream(_socketClient);
                    using (var bufStream = new BufferedStream(netStream))
                    {
                        while (!_token.IsCancellationRequested && _socketClient.Connected)
                        {
                            while (netStream.DataAvailable)
                            {
                                var bytesRead = netStream.Read(byteArray, 0, byteArray.Length);
                                memoryStream.Write(byteArray, 0, bytesRead);
                                Debug.Write($"Received {bytesRead} bytes.");
                            }

                            if (memoryStream.Position > 0)
                            {
                                var size = (int)memoryStream.Position;
                                var bytes = memoryStream.GetBuffer();
                                memoryStream.Position = 0;

                                var request = HttpMessageParser.Parse(bytes, 0, size);

                                var bytesToSend = MakeResponse(request);
                                bufStream.Write(bytesToSend, 0, bytesToSend.Length);
                                bufStream.Flush();
                            }

                            const int oneSecond = 1000000;
                            _socketClient.Poll(oneSecond, SelectMode.SelectRead);
                        }
                    }

                    try
                    {
                        netStream.Close();
                        memoryStream.Close();
                        _socketClient.Shutdown(SocketShutdown.Both);
                        _socketClient.Close();
                    }
                    catch (SocketException e)
                    {
                        Logger.WriteError($"Exception: {e}");
                    }
                });
        }

        private byte[] MakeResponse(IHttpRequest request)
        {
            var route = _routeTable.Find(request.RequestUri);
            request.AddPathParameters(route.PathParameters);

            return route.Resource
                .GetResponse(request)
                .ToBytes();
        }
    }
}