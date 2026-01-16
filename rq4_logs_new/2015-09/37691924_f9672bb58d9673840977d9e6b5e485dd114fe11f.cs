using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO.Pipes;
using System.IO;
using System.Configuration;

namespace Notifier
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Count() != 1)
            {
                Console.WriteLine("arguments length is not equal to 1, press any key to exit");
                var keyInfo = Console.ReadKey();
                return;
            }
            Console.WriteLine(args[0]);
            SendCommand(args[0]);
        }


        private static void SendCommand(string sContent)
        {

            string sProgramName = ConfigurationManager.AppSettings["dstProgramName"];
            using (NamedPipeClientStream pipeClient = new NamedPipeClientStream(".", sProgramName,
                                                                           PipeDirection.Out,
                                                                        PipeOptions.None))
            {
                
                Console.WriteLine("Attempting to connect to pipe...");
                try
                {
                    pipeClient.Connect(1000);
                }
                catch
                {
                    Console.WriteLine("The Pipe server must be started in order to send data to it.");
                    return;
                }
                Console.WriteLine("Connected to pipe.");

                using (StreamWriter sw = new StreamWriter(pipeClient))
                {
                    sw.Write(sContent);
                }
            }
        }
    }
}