using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasySave.Model
{
    public interface ICrypto
    {
        public string ExecutablePath { get; set; }
        public double Crypt(string filePath);
    }
    public class Crypto : ICrypto
    {
        public string ExecutablePath { get; set; }
        public double Crypt(string filePath) {
            using Process crypto = new Process();
            crypto.StartInfo.FileName = ExecutablePath;
            crypto.StartInfo.Arguments = filePath;
            crypto.Start();
            crypto.WaitForExit();
            return crypto.ExitCode;

        }
    }
}