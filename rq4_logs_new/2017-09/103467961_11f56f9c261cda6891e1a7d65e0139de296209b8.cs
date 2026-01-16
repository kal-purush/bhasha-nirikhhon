using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Protocol
{
    public abstract class Protocol
    {
        protected void ReadData(Socket socket)
        {
            var buffer = new byte[100000];
            int iRx = socket.Receive(buffer);
            var chars = new char[iRx];

            Decoder d = Encoding.UTF8.GetDecoder();
            int charLen = d.GetChars(buffer, 0, iRx, chars, 0);

            Console.WriteLine("Message: " + new string(chars));
        }
    }
}