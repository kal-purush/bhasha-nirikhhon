using System.Threading;
using System;
using System.Net;
using System.Net.Sockets;
using System.IO;

namespace server
{
    public class Server
    {

        TcpClient client = null;

        public Server(TcpClient tcpc)
        {
            client = tcpc;
        }

        public void Conversation()
        {
            Console.WriteLine("Connection accepted.");

            NetworkStream stream = client.GetStream();
            StreamReader reader = new StreamReader(stream);
            StreamWriter writer = new StreamWriter(stream);

            string input = reader.ReadLine();

            while (input != "quit")
                {
                    Console.WriteLine("Message received: " + input);
                    writer.WriteLine(input);
                    writer.Flush();
                    Console.WriteLine("Message sent back: " + input);
                    input = reader.ReadLine();
                }

            Console.WriteLine("Closing the connection.");
            reader.Close();
            writer.Close();
            client.Close();
        }

        public static void Main(string[] args)
        {
        TcpListener listener = null;
        
        try
        {
            int port = 5000;
                listener = new TcpListener(IPAddress.Parse("127.0.0.1"), port);
    
                listener.Start();
    
                Console.WriteLine("Server running on port {0}", port);
    
                while (true)
                {
                    Server server = new Server(listener.AcceptTcpClient());
    
                    Thread serverThread = new Thread(new ThreadStart(server.Conversation));
    
                    serverThread.Start();
                }
        }
        catch (Exception e)
        {
            Console.WriteLine(e + " " + e.StackTrace);
        }
        finally
        {
            listener.Stop();
        }
        }
    }
}