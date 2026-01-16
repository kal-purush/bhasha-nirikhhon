using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BakalariClient.Services
{
    class LogService
    {
        private string Filename;

        public LogService(string filename = "log.txt")
        {
            Filename = filename;
        }

        public string GetLogFile()
        {
            string fileContents = "";
            try
            {
                fileContents = File.ReadAllText(Filename);
            }
            catch
            {
                return null;
            }
            return fileContents;
        }

        public void Add(string text)
        {
            string fileContents = GetLogFile();
            fileContents += $"{ DateTime.Now.ToString() }  { text }\n";
            File.WriteAllText(Filename, fileContents);
        }
    }
}