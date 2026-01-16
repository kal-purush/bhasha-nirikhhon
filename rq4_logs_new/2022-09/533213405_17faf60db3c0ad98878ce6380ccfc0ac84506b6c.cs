using ABB.Robotics.Controllers.Discovery;
using ABB.Robotics.Controllers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace data_collection
{
    internal class ABBController
    {
        public ControllerInfoCollection controllers = null;

        public List<string> errLogger(List<string> err, string str)
        {
            err.Add(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "    " + str);
            Console.Write(err.ToArray());
            return err;
        }

        public void Scan()
        {
            NetworkScanner netscan = new NetworkScanner();
            netscan.Scan();
            controllers = netscan.Controllers;

        }

        public Controller GetController(int Select)
        {
            return new Controller(controllers[Select]);
        }

    }
}