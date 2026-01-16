using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    class csClient
    {
        static void Main(string[] args)
        {
            Connect();

        }
        static void Connect()
        {
            try
            {
                // Create a TcpClient.
                // Note, for this client to work you need to have a TcpServer 
                // connected to the same address as specified by the server, port
                // combination.
                Int32 port = 13000;
                TcpClient client = new TcpClient("localhost", port);
                // Translate the passed message into ASCII and store it as a Byte array.
                byte[] data = new byte[256];
               
                Random rand = new Random();

                while (true)
                {   /*This approach is for checking on receiving data whether IDs given to the actuators differs from     those given in 4DIAC.
                      It works by sending an array of bytes where default value is 2 since 0/1 equals false/true respectively.*/
                    /*for (int i = 0; i < data.Length; i++)
                    {
                        data[i] = (int)2;
                    }*/


                    // Randomly generate values for actuators
                    data[0] = Convert.ToByte(rand.Next(2) == 0);
                    data[3] = Convert.ToByte(rand.Next(2) == 0);
                    data[6] = Convert.ToByte(rand.Next(2) == 0);
                    data[9] = Convert.ToByte(rand.Next(2) == 0);

                    NetworkStream stream = client.GetStream();
                    string message = System.Text.Encoding.ASCII.GetString(data);
                    // Send the message to the connected TcpServer. 
                    stream.Write(data, 0, data.Length);
                    Console.WriteLine("Sent: {0}", message);
                    // String to store the response ASCII representation.
                    String responseData = String.Empty;
                    // Read the first batch of the TcpServer response bytes.
                    Int32 bytes = stream.Read(data, 0, data.Length);
                    responseData = System.Text.Encoding.ASCII.GetString(data, 0, bytes);
                    Console.WriteLine("Received: {0}", responseData);

                    /*// Close everything.
                    stream.Close();
                    client.Close();*/
                }
            }
            catch (ArgumentNullException e)
            {
                Console.WriteLine("ArgumentNullException: {0}", e);
            }
            catch (SocketException e)
            {
                Console.WriteLine("SocketException: {0}", e);
            }

            Console.WriteLine("\n Press Enter to continue...");
            Console.Read();
        }
    }
}