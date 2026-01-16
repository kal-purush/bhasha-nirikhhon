using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Entities;

internal enum HttpVerb
{
    Get = 0,
    Post = 1,
    Put = 2,
    Delete = 3,
    Patch = 4,
    Head = 5,
    Options = 6
}

public class MessageHeader
{
}

public class GeneralHeader : MessageHeader
{

}

public class RequestHeader : MessageHeader
{

}

internal interface IHttpRequest
{
    HttpVerb Verb { get; }
    string MessageBody { get; }
    string RequestUri { get; }
    string HttpVersion { get; }
    IEnumerable<MessageHeader> Headers { get; }

    void SetResponse(int code, string body);
}

internal class HttpRequest : IHttpRequest
{
    private Socket _httpClient;

    public HttpVerb Verb { get; set; }

    public string MessageBody { get; set; }

    public string RequestUri { get; set; }

    public string HttpVersion { get; set; }

    public IEnumerable<MessageHeader> Headers => new List<MessageHeader>();

    public HttpRequest(Socket httpClient)
    {
        _httpClient = httpClient;
    }

    public void SetResponse(int code, string body)
    {

    }
}

internal class HttpServer
{
    private int _port;
    private IPAddress _ipAddress;

    public HttpServer(int port)
    {
        _port = port;
        string hostName = Dns.GetHostName();
        var ipHostInfo = Dns.GetHostEntry(hostName);
        _ipAddress = null;

        for (int i = 0; i < ipHostInfo.AddressList.Length; ++i)
        {
            if (ipHostInfo.AddressList[i].AddressFamily ==
              AddressFamily.InterNetwork)
            {
                _ipAddress = ipHostInfo.AddressList[i];
                break;
            }
        }

        if (_ipAddress == null)
            throw new Exception("No IPv4 address for server");
    }

    public async void Start()
    {
        var listener = new HttpListener(_ipAddress, _port);
        listener.Listen();

        while (true)
        {
            try
            {
                var httpClient = await listener.AcceptAsync();
                var exchange = Serve(httpClient);
                await exchange;
            }
            catch (Exception ex)
            {
                Logger.WriteError($"Communication error: {ex}");
            }

        }
    }

    private async Task Serve(Socket httpClient)
    {
        var size = 512;
        var readbuffer = new byte[size];
        var bufferArr = new ArraySegment<byte>(readbuffer);
        var membuffer = new List<byte>();

        try
        {
            httpClient.SetSocketOption(
                SocketOptionLevel.Socket,
                SocketOptionName.KeepAlive,
                true);

            while (httpClient.Connected)
            {
                var num = await httpClient.ReceiveAsync(bufferArr, 0);
                Debug.Write($"Http Listener received {num} bytes from client.");
                if (num != 0)
                {
                    membuffer.AddRange(readbuffer.Take(num));
                    if (num < size)
                    {
                        var request = Parse(membuffer.ToArray());
                        //var response = await GetResponse(request);
                        var response = GetResponse(request);
                        var bytes = Encoding.ASCII.GetBytes(response);
                        var bytesArr = new ArraySegment<byte>(bytes);
                        await httpClient.SendAsync(bytesArr, 0);
                        Debug.Write($"Http Listener sent {bytes.Length} bytes to client.");
                    }
                }
                else
                {
                    break; // nothing else to share
                }
            }
            httpClient.Close();
        }
        catch (Exception ex)
        {
            Logger.WriteError($"Connection error while serving client: {ex}");

            if (httpClient.Connected)
                httpClient.Close();
        }
    }

    //private async Task<string> GetResponse(IHttpRequest request)
    private string GetResponse(IHttpRequest request)
    {
        var response = "<html>Weicome to my http server 0.1</html>";

        return "HTTP/1.1 200 OK\r\n" +
               "Server: ES-HttpServer v0.1\r\n" +
               "Content-Type: text/html; charset=UTF-8\r\n" +
               $"Content-Length: {response.Length}\r\n" +
               "\r\n" +
               response;
    }

    private IHttpRequest Parse(byte[] bytes)
    {
        var message = Encoding.ASCII.GetString(bytes);
        var extract = message.Substring(0, Math.Min(message.Length, 500));
        Debug.Write($"Parsing raw request (showing max 500 characters):" +
                    $"\r\n{extract}");

        return new HttpRequest(null)
        {
            Verb = HttpVerb.Get,
            RequestUri = "*"
        };
    }
}


internal class HttpListener : IListener
{
    private IPAddress _ipAddress;
    private int _port;
    private EventWaitHandle _stop;
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
    }

    public async Task<Socket> AcceptAsync()
    {
        var clientSocket = await _serverSocket.AcceptAsync();
        Logger.WriteInfo($"Connection accepted from {clientSocket.LocalEndPoint}");
        return clientSocket;
    }
}
