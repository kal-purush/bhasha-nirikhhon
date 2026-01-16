using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Web;

namespace MVCWebApplication.Common
{
    public class Log
    {
        public void WriteError(string msg, Exception ex)
        {
            string logPath = ConfigurationManager.AppSettings.Get(Const.Config.LOG_PATH).ToString();
            string logFileName = "webapplication.log";
            string logFilePath = Path.Combine(logPath, logFileName);
            //UTF8で書き込む
            //書き込むファイルが既に存在している場合は、上書きする
            using (StreamWriter sw = new StreamWriter(logFilePath, true, Encoding.UTF8))
            {
                string logDateTime = DateTime.Now.ToString();
                sw.WriteLine("-> " + logDateTime);
                sw.WriteLine("-> " + msg);
                if (ex != null)
                {
                    sw.WriteLine("-> " + ex.Message);
                    if (ex.InnerException != null)
                    {
                        sw.WriteLine(ex.InnerException.Message);
                    }
                }
                //閉じる
                sw.Close();
            }
        }
    }
}