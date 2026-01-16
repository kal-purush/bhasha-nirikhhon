using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EscapeCharacterOrnek
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello\nWorld");
            Console.WriteLine("Hello\tWorld");
            Console.WriteLine("\"Hello World\""); // "Hello World"
            Console.WriteLine("c:\\users\\nuh\\appdata"); // c:\users\tolgay\appdata - \n \t \" \' \\ @ \u1234
            Console.WriteLine(@"c:\users\nuh\appdata"); // c:\users\tolgay\appdata  - @ işareti koyduktan sonra backward slash işareti olmadan string yazabiliyorsunuz.
            Console.ReadLine();
        }
    }
}