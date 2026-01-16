using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace getAllFilesFromGivenDirectory
{
    class Program
    {
        public static void getZipFilesRecursively(string targetDir)
        {
            string[] subDirs = Directory.GetDirectories(targetDir);
            
            HashSet<string> extensions = new HashSet<string> { ".gz", ".7z", ".zip", ".tar" };
            string[] filePahts = Directory.GetFiles(targetDir);

            foreach (string filePath in filePahts)
            {                
                string fileName = Path.GetFileName(filePath);
                string fileExt = Path.GetExtension(fileName);
                if (extensions.Contains(fileExt))
                {
                    processFile(fileName);      
                }
            }

            foreach (string subDir in subDirs)
            {
                getZipFilesRecursively(subDir);
            }
        }

        private static void processFile(string fileName)
        {
            Console.WriteLine("File = " + fileName);
        }

        static void Main(string[] args)
        {
            string targetDir = "C:/Users/Mahathir/Desktop/test";
            getZipFilesRecursively(targetDir);
        }
    }
}