using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;

namespace NewsDistribution;

public class NewsClient
{
    private Socket? _socket;
    private NetworkStream? _stream;
    private readonly byte[] _buffer = new byte[2048];
    Thread? _receiveThread;

    public delegate void NewsReceived(string title, string description, string content);
    public event NewsReceived? OnNewsReceived;

    public bool Connect(string name, string address, ushort port)
    {
        if (name == string.Empty || name.Length > 256)
            throw new ArgumentException("Name is either empty or too long (>256).", nameof(name));

        if (_socket != null)
            return false;

        try
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(address, port);
        }
        catch (SocketException)
        {
            _socket?.Dispose();
            _socket = null;

            return false;
        }

        _stream = new(_socket);

        SendAuthorizationPacket(name);
        bool authorized = ReceiveAuthorizationPacket();

        if (authorized)
        {
            _receiveThread = new Thread(ReceiveThreadProc);
            _receiveThread.Start();
        }
        else
        {
            _stream.Close();
            _socket.Close();
            _stream = null;
            _socket = null;
        }

        return authorized;
    }

    public void Disconnect(bool sendPacket = true)
    {
        if (_socket == null)
            return;

        if (sendPacket)
        {
            _buffer[0] = (byte)PacketType.Unsubscribe;
            _socket.Send(_buffer, 1, SocketFlags.None);
        }

        _socket.Disconnect(false);
        _stream!.Close();
        _socket = null;
    }

    private void ReceiveThreadProc()
    {
        if (_socket == null)
            return;

        while (true)
        {
            _socket.Receive(_buffer, 1, SocketFlags.None);

            switch ((PacketType)_buffer[0])
            {
                case PacketType.Unsubscribe:
                    Disconnect(false);
                    break;

                case PacketType.News:
                    ReceiveNewsPacket();
                    break;
            }
        }
    }

    private void SendAuthorizationPacket(string name)
    {
        if (_socket == null)
            throw new InvalidOperationException();

        byte[] nameBytes = Encoding.UTF8.GetBytes(name);
        int length = nameBytes.Length;

        if (length > 256)
            return;

        _stream!.WriteByte((byte)length);
        _stream.Write(nameBytes);
        _stream.Flush();
    }

    private bool ReceiveAuthorizationPacket()
    {
        if (_socket == null)
            throw new InvalidOperationException();

        _socket.Receive(_buffer, 1, SocketFlags.None);

        return _buffer[0] != 0;
    }

    private void ReceiveNewsPacket()
    {
        if (_socket == null)
            throw new InvalidOperationException();

        using var reader = new BinaryReader(_stream!);

        string title = reader.ReadString();
        string Description = reader.ReadString();
        string Content = reader.ReadString();

        OnNewsReceived?.Invoke(title, Description, Content);
    }
}