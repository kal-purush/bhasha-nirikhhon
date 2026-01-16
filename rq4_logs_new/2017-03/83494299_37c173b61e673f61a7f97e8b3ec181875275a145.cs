using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;


namespace Menu_seguridad
{
   public class ClaseTomaIp
    {

        public string direccion()
        {

            string localIP = "";
            IPHostEntry host;
            
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily.ToString() == "InterNetwork")
                {
                    localIP = ip.ToString();
                    return localIP;
                }
                
            }
            return localIP;
        }

    }
}