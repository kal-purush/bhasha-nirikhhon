using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DirectoryInfoClass
{
    internal class Program
    {
        static void Main(string[] args)
        {

            string path = @"D:\C#_Basic_Local\FileInputOutput\My Directory"; //verbatim Literal
            string path2 = @"D:\C#_Basic_Local\FileInputOutput\My Directory 2"; //verbatim Literal

            DirectoryInfo dir = new DirectoryInfo(path);
            dir.Create();
            Console.WriteLine("Directory created");
           // create sub directory
            dir.CreateSubdirectory("Another Directory");
            Console.WriteLine("Another Diredtory created");
            dir.MoveTo(path2);

            Console.WriteLine(" Directory Moved .");

            dir.Delete(); //if data is not present than dir is not directoyr
            dir.Delete(true);  //directory delete
            DirectoryInfo[] dirs = dir.GetDirectories();
            foreach (DirectoryInfo dirInfo in dirs)
            {
                Console.WriteLine(dirInfo.FullName);
            }
            

            Console.WriteLine();
            foreach (DirectoryInfo dirInfo in dirs)
            {
                Console.WriteLine(dirInfo.GetFiles().Length);
            }
            Console.WriteLine("Propertis");

            Console.WriteLine(dir.Name);
            Console.WriteLine(dir.FullName);
            Console.WriteLine(dir.LastAccessTime);
            Console.WriteLine(dir.CreationTime);
            

        }
    }
}