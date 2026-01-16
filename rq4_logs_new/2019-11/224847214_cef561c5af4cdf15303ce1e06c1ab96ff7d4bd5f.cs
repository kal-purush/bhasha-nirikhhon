using System;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace UDPGolf
{
    class Program
    {
        static void Main(string[] args)
        {
            //opsætning af UDP socket
            //OBS: husk at ændre IP hvis nødvendigt!
            UdpClient udpGolfServer = new UdpClient(6789);
            IPAddress ip = IPAddress.Parse("192.168.104.124");
            IPEndPoint golfIpEndPoint = new IPEndPoint(ip, 6789);

            
            try
            { Console.WriteLine("Server is waiting for input");

                while (true)
                {
                    //modtagelse af data fra Raspberry Pi'en, som så printes ud i konsollen. 
                    Byte[] receivedData = udpGolfServer.Receive(ref golfIpEndPoint);
                    string receivedSwingData = Encoding.ASCII.GetString(receivedData);
                    Console.WriteLine("Received Swing Data is: " + receivedSwingData + " km/t.");




                }
            }
            catch (Exception e)
            {
                //hvis noget går galt udskrives exceptionen her
                Console.WriteLine(e);
            }
        }
    }
}