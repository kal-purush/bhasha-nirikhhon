using System;
using System.Net;
using System.Net.Sockets;
using System.IO;
using HTTPServerProject.Interfaces;

namespace HTTPServerProject.Request.Body
{
    public class Body
    {
        NewStreamReader reader = default;

        public Body(NewStreamReader r)
        {
            reader = r;
        }

        public string GetBody()
        {
            var input = reader.Read();
            var result = "";
            while(input != -1)
            {
                char c = (char)input;
                result += c;
                input = reader.Read();
            }
            return result;
        }
    }
}