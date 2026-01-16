using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FileHandler
{
    public class FileReader
    {
        public string[] ReadFromFileInRootDirectory(string fileNameWithoutExtension)
        {
            string rootDirectory = Directory.GetCurrentDirectory();
            string fullPath = $"{rootDirectory}\\{fileNameWithoutExtension}";
            string[] fileContent = File.ReadAllLines(fullPath);
            return fileContent;
        }
        public string[] ReadFromFileInRootDirectory(string fileNameWithoutExtension, string pathInRootDirectory)
        {
            string rootDirectory = Directory.GetCurrentDirectory();
            string fullPath = $"{rootDirectory}\\{pathInRootDirectory}\\{fileNameWithoutExtension}.txt";
            string[] fileContent = File.ReadAllLines(fullPath);
            return fileContent;
        }
    }
}