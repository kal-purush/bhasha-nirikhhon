using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace RoswellSignatureSync
{
    public partial class RoswellSignatureSyncService : ServiceBase
    {

        string logSourceName = "RoswellSignatureSync";
        string logName = "RoswellSignatureSync";

        public RoswellSignatureSyncService()
        {
            this.AutoLog = false;
            InitializeComponent();

            // Define event log
            signatureSyncLog = new System.Diagnostics.EventLog();
            if (!System.Diagnostics.EventLog.SourceExists(logSourceName))
            {
                System.Diagnostics.EventLog.CreateEventSource(
                    logSourceName, logName);
            }
            signatureSyncLog.Source = logSourceName;
            signatureSyncLog.Log = logName;
        }

        protected override void OnStart(string[] args)
        {
        }

        protected override void OnStop()
        {
        }
    }
}