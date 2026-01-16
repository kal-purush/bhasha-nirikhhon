using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Sockets;
using System.IO;
using System.Globalization;
namespace getTimeInternet
{
    class Program
    {
        int a;

        static void Main(string[] args)
        {
            var client = new TcpClient("time.nist.gov", 13);
            while (true)
            {
                GetTime(client);
                Console.Clear();
            }
            

        }

        private static void GetTime(TcpClient cclient)
        {
            var client = new TcpClient("time.nist.gov", 13);
            using (var streamReader = new StreamReader(client.GetStream()))
            {

                var response = streamReader.ReadToEnd();
                if (response == "")
                    return;
                var utcDateTimeString = response.Substring(7, 17);
                var localDateTime = DateTime.ParseExact(utcDateTimeString, "yy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
                int a = 3;
                a = 5;
                Console.WriteLine("hello {0}", utcDateTimeString);

            }
        }
    }
}