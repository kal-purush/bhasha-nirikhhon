using System;
using System.IO;
using System.Reflection;

namespace Labs
{
    class Program
    {
        static void Main(string[] args)
        {
            string filePath = Path.Combine(Directory.GetCurrentDirectory(), "Список.csv");
            string inputFilePath = Path.Combine(Directory.GetCurrentDirectory(), "input.bin");
            string outputFilePath = Path.Combine(Directory.GetCurrentDirectory(), "output.bin");

            string[] lines = File.ReadAllLines(filePath);

            BinaryWriter bw = new BinaryWriter(new FileStream(inputFilePath, FileMode.Create));
            
            bw.Close();

            BinaryReader br = new BinaryReader(new FileStream(inputFilePath, FileMode.Open));

            br.Close();

            BinaryWriter bw1 = new BinaryWriter(new FileStream(outputFilePath, FileMode.Create));

            bw1.Close();
        }
    }
}