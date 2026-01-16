using System;
using System.IO;

namespace ConsoleApplication1
{
    class FileExample
    {
        static void Main()
        {
            FileStream fs = new FileStream(@"E:\Training\MS .NET\Week - 9\data.txt", FileMode.Append, FileAccess.Write);

            StreamWriter writer = new StreamWriter(fs);

            string message;
            Console.WriteLine("Enter some text:");
            message = Console.ReadLine();
            writer.WriteLine(message);

            writer.Flush();
            writer.Close();
            fs.Close();

            Console.ReadLine();
        }
    }

}