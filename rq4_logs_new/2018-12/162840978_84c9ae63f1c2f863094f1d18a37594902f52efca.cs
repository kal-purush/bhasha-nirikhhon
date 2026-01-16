using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpamTools.lib.Data
{
    public class MailServer
    {
        public MailServer() { }

        public MailServer(string adress, int port = 25, bool useSSL = true)
        {
            Adress = adress;
            Port = port;
            UseSSL = useSSL;
        }

        public string Adress { get; set; }
        public int Port { get; set; } = 25;
        public bool UseSSL { get; set; } = true;
    }
}