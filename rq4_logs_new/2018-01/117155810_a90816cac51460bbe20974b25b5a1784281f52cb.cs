using System;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace SharpNetwork
{
    public class NetworkClient
    {
        private readonly string _hostname;
        private readonly int _port;

        private readonly TcpClient _tcpClient;
        private readonly Stream _stream;

        private byte[] _readBuffer;

        /**
         * Creates a new client and connects to the specified host, then sets the stream variable and starts listening.
         */
        public NetworkClient(string hostname, int port, int bufferSize = 4096)
        {
            // -- Define private variables
            _hostname = hostname;
            _port = port;

            // -- Set up tcp client
            _tcpClient = new TcpClient();

            // -- Connect to host
            _tcpClient.Connect(_hostname, _port);

            // -- Get stream from tcp client
            _stream = _tcpClient.GetStream();

            // -- Init readBuffer
            _readBuffer = new byte[bufferSize];

            // -- Start listening
            _stream.BeginRead(_readBuffer, 0, _readBuffer.Length, HandleRead, null);
        }

        /**
         * Delegate callback when data is received.
         */
        private void HandleRead(IAsyncResult result)
        {
            // -- Checking
            if (!_tcpClient.Connected)
                return;

            if (!result.IsCompleted)
                return;

            // -- Get number of bytes in received data
            int numberOfBytes = _stream.EndRead(result);

            // -- Decode bytes to string - from UTF8
            string message = Encoding.UTF8.GetString(_readBuffer, 0, numberOfBytes);

            // -- Call virtual method - OnMessageReceived
            OnMessageReceived(message);
        }

        /**
         * Virtual method to deal with received message.
         * Called from HandleRead delegate.
         */
        protected virtual void OnMessageReceived(string message)
        {
            Console.WriteLine(message);
        }

        /**
         * Checks if the client is connected, and closes it if it is.
         */
        public void Close()
        {
            if(!_tcpClient.Connected)
                return;

            _tcpClient.Close();
        }
    }
}