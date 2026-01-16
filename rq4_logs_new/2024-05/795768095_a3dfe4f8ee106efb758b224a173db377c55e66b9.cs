using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary
{
    public class FileAdapter : Logger
    {
        private FileWriter _fileWriter;

        public FileAdapter()
        {
            _fileWriter = new FileWriter();
        }

        public new void Log(string message)
        {
            _fileWriter.WriteLine("Log: " + message);
        }

        public new void Error(string message)
        {
            _fileWriter.WriteLine("ERROR: " + message);
        }

        public new void Warn(string message)
        {
            _fileWriter.WriteLine("WARNING: " + message);
        }
    }
}