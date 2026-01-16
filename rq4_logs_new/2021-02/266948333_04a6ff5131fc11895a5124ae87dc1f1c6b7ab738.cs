using FPOSDB.Context;
using FPOSPriceUpdater.DTO;
using log4net;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FPOSPriceUpdater.BusinessLogic
{
    public class Export : Transferable, ITransferable
    {
        public Export(ILog log) : base(log)
        {
        }

        private bool IsPathSet()
        {
            if (String.IsNullOrEmpty(Path))
            {
                UpdateStatus(StatusMessage.Create(TransferStatus.Failed, "Failed!" + Environment.NewLine + "Export path is missing or invalid",new ArgumentException()));
                return false;
            }
            return true;
        }
        private bool IsDbConnected()
        {
            DBService db = new DBService(ConnectionString.GetString());
            if (!db.IsConnectionValid())
            {
                UpdateStatus(StatusMessage.Create(TransferStatus.Failed, "Failed!" + Environment.NewLine + "Check your database connection.",new ApplicationException()));
                return false;
            }
            return true;
        }

        public bool IsReady()
        {
            return (IsPathSet() && IsDbConnected());
        }


        public void UpdateStatus(StatusMessage status)
        {
            switch (status.Status)
            {
                case TransferStatus.Started:
                    this.IsNotRunning = false;
                    this.Status = status.Message;
                    Log.Info("***** Export Started *****");
                    break;
                case TransferStatus.Failed:
                    this.IsNotRunning = true;
                    this.Status = status.Message;
                    Log.Error(status.Message, status.Exception);
                    Log.Error("***** Export Failed *****");
                    break;
                case TransferStatus.Stopped:
                    this.IsNotRunning = true;
                    Log.Info("***** Export Stopped *****");
                    break;
                case TransferStatus.Success:
                    this.IsNotRunning = true;
                    this.Status = status.Message;
                    Log.Info(status.Message);
                    Log.Info("***** Export Successful *****");
                    break;
                case TransferStatus.Reset:
                    this.IsNotRunning = true;
                    this.Status = status.Message;
                    break;
                default:
                    this.IsNotRunning = true;
                    this.Status = status.Message;
                    Log.Error(status.Message);
                    Log.Error("***** Export ERROR*****");
                    break;
            }
        }
    }
}