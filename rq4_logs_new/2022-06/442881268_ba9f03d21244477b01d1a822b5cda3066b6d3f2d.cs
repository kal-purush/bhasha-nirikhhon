using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Updater.CustomElements
{
    internal class CheckStatusWorker : BackgroundWorker
    {
        public string Key { get; set; }
        public string ProjectName { get; set; }
        public string Status { get; set; }
        public string Link { get; set; }
        public string StatusType { get; set; }
        public string FailedMessage { get; set; }
        public string SuccessMessage { get; set; }
        public string RequestLink { get; set; }
    }
}