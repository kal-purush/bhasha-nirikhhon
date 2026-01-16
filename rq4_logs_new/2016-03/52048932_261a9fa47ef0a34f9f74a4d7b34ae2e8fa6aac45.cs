using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace StreamReaderWriter
{
  class ReaderWriter
  {
    static void Main(string[] args)
    {
      ReadStream();
    }

    static void WriteStream()
    {
      using (StreamWriter writer = new StreamWriter(@"C:\OutputFiles\important.txt"))   
      {
        writer.Write("Word ");
        writer.WriteLine("word 2");
        writer.WriteLine("Line");
      }
    }

    static void ReadStream()
    {

      using (StreamReader reader = new StreamReader(@"C:\OutputFiles\important.txt"))
      {
        string line;
        while ((line = reader.ReadLine()) != null)
        {
          Console.WriteLine(line); // Write to console.
        }
      }

    }

  }
}