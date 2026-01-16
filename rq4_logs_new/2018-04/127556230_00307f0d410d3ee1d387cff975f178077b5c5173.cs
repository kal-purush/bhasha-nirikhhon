using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NotifactionSender;
using System.IO;

namespace FileWriter
{
    public class FileWriter : ISender
    {
        private string _path;

        public void SetDestenation(string path)
        {
            _path = path;
        }
        public void Send(string message)
        {
            File.WriteAllText(_path, message);
        }
    }
}