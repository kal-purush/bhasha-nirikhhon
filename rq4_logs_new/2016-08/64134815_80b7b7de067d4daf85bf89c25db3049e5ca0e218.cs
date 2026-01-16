using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HTTPServer.test
{
    public class DataHolder
    {
        static MemoryStream memory = new MemoryStream();
        private static int datalength;

        public static int WriteData(byte[] buffer)
        {
            datalength = buffer.Length;
            memory.Write(buffer,0,datalength);
            return datalength;
        }

        public static int ReadData(byte[] buffer)
        {
            memory.Position = 0;
            memory.Read(buffer, 0, datalength);
            memory = new MemoryStream();
            return datalength;
        }
    }
}