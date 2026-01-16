using System;
using System.IO;

namespace FileIO
{
    public class Program
    {
        public static void Main()
        {
            try
            {
                string path = @"C:\Users\ManasaVM\source\repos";
                string[] files = Directory.GetFiles(path);
                string[] directories = Directory.GetDirectories(path);
                Console.WriteLine($"Files in {path}\n");
                foreach (string file in files)
                {
                    string filename = Path.GetFileName(file);
                    Console.WriteLine(filename);
                    string filepath = Path.Combine(path, filename);
                    FileInfo myfile = new FileInfo(filepath);
                    StreamReader sr = myfile.OpenText();
                    string data = "";
                    while ((data = sr.ReadLine()) != null)
                    {
                        Console.Write(data);

                    }
                    Console.WriteLine("\n");

                }
                Console.WriteLine($"Subdirectories inside {path}\n");
                foreach (string directory in directories)
                {
                    DirectoryInfo directoryinfo = new DirectoryInfo(directory);
                    directoryinfo.GetDirectories();
                    string directoryName = directoryinfo.Name;
                    Console.WriteLine(directoryName);
                }
            }
            catch (IOException e)
            {
                Console.WriteLine(e);
            }
        }
    }
}