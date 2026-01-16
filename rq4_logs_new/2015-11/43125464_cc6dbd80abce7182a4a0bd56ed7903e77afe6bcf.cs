using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace SocketExtensions
{
    public class AsyncSocket : IDisposable
    {
        public const int BUFFER_LENGTH = 0x1000;

        private bool _disposed = false;
        private Socket _socket;

        private SocketAwaitableEventWrapper _sender, _receiver;

        public AsyncSocket(Socket s)
        {
            this._socket = s;
            this._sender = new SocketAwaitableEventWrapper();
            this._receiver = new SocketAwaitableEventWrapper();
            this._receiver.EventArgs.SetBuffer(new byte[BUFFER_LENGTH], 0, BUFFER_LENGTH);
        }

        public async Task<ReadResult> ReceiveAsync()
        {
            await _socket.ReceiveAsync(_receiver.SocketAwaitable);
            int bytesRead = _receiver.EventArgs.BytesTransferred;
            byte[] buffer = new byte[bytesRead];
            Array.Copy(_receiver.EventArgs.Buffer, buffer, bytesRead);
            return new ReadResult(bytesRead, buffer);
        }

        public async Task SendMessage(byte[] buffer)
        {
            _sender.EventArgs.SetBuffer(buffer, 0, buffer.Length);

            await _socket.SendAsync(_sender.SocketAwaitable);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool safeToFreeManaged)
        {
            if (!_disposed)
            {
                if (safeToFreeManaged)
                {
                    _socket?.Close();
                    _receiver?.Dispose();
                    _sender?.Dispose();
                }
                // free unmanaged resources here.

                _disposed = true;
            }
        }

        /* ~AsyncSocket() // Enable if AsyncSocket is to contain unmanaged resources.
         {
             Dispose(false);
         }*/

        public struct ReadResult
        {
            public int BytesRead;
            public byte[] Buffer;

            public ReadResult(int bytesRead, byte[] buffer)
            {
                this.BytesRead = bytesRead;
                this.Buffer = buffer;
            }
        }

        private class SocketAwaitableEventWrapper
        {
            private bool _disposed = false;

            public SocketAsyncEventArgs EventArgs { get; }
            public SocketAwaitable SocketAwaitable { get; }

            public SocketAwaitableEventWrapper()
            {
                EventArgs = new SocketAsyncEventArgs();
                SocketAwaitable = new SocketAwaitable(EventArgs);
            }

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected virtual void Dispose(bool safeToFreeManaged)
            {
                if (!_disposed)
                {
                    if (safeToFreeManaged)
                    {
                        EventArgs?.Dispose();
                    }
                    // free unmanaged resources here.

                    _disposed = true;
                }
            }
        }
    }
}