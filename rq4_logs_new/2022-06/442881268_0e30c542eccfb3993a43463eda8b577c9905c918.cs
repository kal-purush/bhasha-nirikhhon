using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Updater.CustomElements
{
    internal class CheckStatusJenkinsWorker : BackgroundWorker
    {
        public string ProjectName { get; set; }
        public string RegisterName { get; set; }
        public string Branch { get; set; }
        public string Stand { get; set; }
        public string Status { get; set; }
        public string Link { get; set; }
        public string StatusType { get; set; }
        public string FailedMessage { get; set; }
        public string SuccessMessage { get; set; }
    }
}