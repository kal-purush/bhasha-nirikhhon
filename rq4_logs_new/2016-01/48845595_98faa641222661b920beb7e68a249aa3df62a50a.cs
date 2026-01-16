using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNet.Mvc;

namespace BBK.App.Controllers
{
    public class MonitoringController : Controller
    {
        // GET: server-status
        [HttpGet("server-status")]
        public ServerStatusMessage ServerStatus()
        {
            return new ServerStatusMessage("OK");
        }
    }

    public class ServerStatusMessage
    {
        public ServerStatusMessage(string message)
        {
            Message = message;
        }

        public string Message { get; private set; }
    }
}