using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    public class ClientController
    {
        private IClientRequest clientRequestManager;

        public ClientController(IClientRequest clientRequestManager)
        {
            this.clientRequestManager = clientRequestManager;
        }

        public void ConnectToServer()
        {
            clientRequestManager.ConnectToServer();
        }
    }
}