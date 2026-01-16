using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Protocol
{
    /*
        using Newtonsoft.Json;
        Client client = new Client("2guti2", "123456");
        string json = JsonConvert.SerializeObject(client);
        Console.WriteLine(json);
        Console.Read();
        Client tmp = JsonConvert.DeserializeObject<Client>(json);
    */
    public abstract class Protocol
    {
        protected const int FixedIntByteLength = 4;

        protected string ReadData(Socket socket)
        {
            byte[] lengthbuffer = new byte[FixedIntByteLength];
            socket.Receive(lengthbuffer);

            int messageLength = BitConverter.ToInt32(lengthbuffer, 0);

            Console.WriteLine("Message length = " + messageLength + " bytes");

            byte[] buffer = new byte[messageLength];
            int iRx = socket.Receive(buffer);
            char[] chars = new char[iRx];

            Decoder d = Encoding.UTF8.GetDecoder();
            int charLen = d.GetChars(buffer, 0, iRx, chars, 0);

            var message = new String(chars);

          //  Console.WriteLine("Message: " + message);

            return message;
        }
    }
}