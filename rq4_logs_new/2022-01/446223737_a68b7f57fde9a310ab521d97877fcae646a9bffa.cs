using System;
using System.IO;
using System.Net;
using System.Linq;
using System.Configuration;
using System.Reflection;
using System.Diagnostics;
using System.Collections.Generic;
using Newtonsoft.Json;
using RatesDAL;
using RatesDAL.Classes;

namespace Rates
{
    class Program
    {
        #region Private properties for the windows task, configuration and general setup properties

        // Main data model class to consume rates url and deserialize JSON to a C# class representation
        private class RatesModel
        {
            public IDictionary<string, float> Rates { get; set; }
            public string Base { get; set; }
            public string Date { get; set; }
        }

        private static string RatesDB
        {
            get
            {
                string str = ConfigurationManager.AppSettings["RatesDB"].ToString();
                return (string.IsNullOrWhiteSpace(str) ? string.Empty : str);
            }
        }

        private static string Currency
        {
            get
            {
                string str = ConfigurationManager.AppSettings["Currency"].ToString();
                return (string.IsNullOrWhiteSpace(str) ? string.Empty : str);
            }
        }

        private static string RatesUrl
        {
            get
            {
                string str = ConfigurationManager.AppSettings["RatesUrl"].ToString();
                return (string.IsNullOrWhiteSpace(str) ? string.Empty : str);
            }
        }

        private static string pConfigFile
        {
            get
            {
                string sConfig = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile;
                return (sConfig);
            }
        }

        private static string pLogFile
        {
            get
            {
                string sPath = Path.GetDirectoryName(pConfigFile);
                string sLog = Path.GetFileNameWithoutExtension(pConfigFile);
                sLog = Path.GetFileNameWithoutExtension(sLog) + ".log";
                return (Path.Combine(sPath, sLog));
            }
        }

        private static string pAppName
        {
            get
            {
                string sName = Assembly.GetEntryAssembly().GetName().Name.ToString();
                string sVersion = Assembly.GetEntryAssembly().GetName().Version.ToString();
                return (sName + " v" + sVersion);
            }
        }

        #endregion

        #region Private utility methods for the windows task, windows event log and logging

        private static void WinEventLogInfo(string msg)
        {
            string message = pAppName + " -> " + msg;
            EventLog.WriteEntry(".NET Runtime", message, EventLogEntryType.Information, 1000);
        }

        private static void WinEventLogWarning(string msg)
        {
            string message = pAppName + " -> " + msg;
            EventLog.WriteEntry(".NET Runtime", message, EventLogEntryType.Warning, 1000);
        }

        private static void WinEventLogError(string msg)
        {
            string message = pAppName + " -> " + msg;
            EventLog.WriteEntry(".NET Runtime", message, EventLogEntryType.Error, 1000);
        }

        /// <summary>
        /// Writes an entry to the passed log file, will create any path as needed for the log file.
        /// </summary>
        private static void WriteToLogFile(string file, string msg)
        {
            // Ensure the path for the log file exists before writing to the log file
            string path = Path.GetDirectoryName(file);
            if (!Directory.Exists(path))
                Directory.CreateDirectory(path);

            using (FileStream fs = new FileStream(file, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
            using (StreamWriter sw = new StreamWriter(fs))
            {
                DateTime dt = DateTime.Now;
                string dtTime = string.Format("{0:00}:{1:00}:{2:00}.{3:000}",
                    (int)(dt.Hour), dt.Minute, dt.Second, dt.Millisecond);
                string sLogEntry = dt.ToString("dd/MM/yyyy ") + dtTime + " -> " + msg;

                sw.WriteLine(sLogEntry);
                sw.Flush();
                sw.Close();
            }
        }

        /// <summary>
        /// Combined logging routine, wrties to the standard windows event log AND a local log file.
        /// </summary>
        private static void WriteLog(string msg)
        {
            // Sometimes servers have permission problems for local log files depending on the service account they
            // are attached too for a windows task, force log entry to standard windows event log as well.
            WinEventLogInfo(msg);
            WriteToLogFile(pLogFile, msg);
        }

        #endregion

        #region Data processing functions and main program entry point, can be run as windows task

        private static void ProcessCurrency(DataAccessLayer db, string data)
        {
            var rates = JsonConvert.DeserializeObject<RatesModel>(data);
            var lookup = db._RatesLookupRepo.GetAll();

            // The class definitions defined in DAL project attached follows WPF standard (MVVM design pattern)
            // and easily allows the classes to be reused from this DAL in any WPF client application.
            var rb = new RatesBase()
            {
                Currency = rates.Base,
                Trade = false,
                Country = lookup.Where(l => l.Code == rates.Base).FirstOrDefault().Id,
                Created = DateTime.Now,
                CUser = "SYSTEM"
            };

            db._RatesBaseRepo.CreateAuto(rb);

            // Not ideal this, grab the auto-increment ID for the newly created RatesBase record
            rb = db._RatesBaseRepo.GetTop(1, "created", true).FirstOrDefault();

            int added = 0;
            foreach (var r in rates.Rates)
            {
                // When validating the data only include in the rates records currencies that differ from the
                // base currency, the same currency pair is pointless.
                if (r.Key != rb.Currency)
                {
                    // For large amounts of records for integration or back office data processing this is an
                    // example of a cached SQL transaction in this repository design pattern. Once a record is
                    // cached a bulk commit then sends all the cached SQL statements, insert/update, in a single
                    // database operation rather than looping through all records and having a single database
                    // operation for each record. StackOverflow's Dapper ORM library is used, it is significantly
                    // faster than Entity Framwework, speed is important for integration/back office data tasks.
                    db._RatesCurrencyRepo.BulkCreate(new RatesCurrency()
                    {
                        BaseId = rb.Id,
                        Currency = r.Key,
                        Value = (float)Math.Round(r.Value, 4), // transform to 4 decimal places
                        Country = lookup.Where(l => l.Code == r.Key).FirstOrDefault().Id,
                        Created = rb.Created,
                        CUser = rb.CUser
                    });

                    added++;
                }
            }
            WriteLog($"Bulk processing exchange rate record(s) complete.");

            // Bulk commit of SQL insert statements wrapped in a transaction, on error condition rollback occurs,
            // profiling has shown this implementation does around 20000 SQL insert/updates in approx 4 seconds.
            db._RatesCurrencyRepo.BulkCommit();
            WriteLog($"Bulk SQL commit of {added} exchange rate record(s).");
        }

        static void Main(string[] args)
        {
            try
            {
                WriteLog($"{pAppName}");

                if (RatesDB != string.Empty && Currency != string.Empty && RatesUrl != string.Empty)
                {
                    var database = new DataAccessLayer(RatesDB);
                    var currencies = Currency.Split(',');

                    foreach (var c in currencies)
                    {
                        WriteLog($"Processing base currency: {c}");

                        // Loop through all currencies in the CSV list from the config file
                        string url = $"{RatesUrl}?base={c}";
                        WriteLog($"Consuming URL: {url}");
                        var client = new WebClient();
                        var response = client.DownloadString(url);

                        ProcessCurrency(database, response);
                    }
                }
                else
                {
                    WriteLog("Error in the app.config file, not enough parameters to run task.");
                }

                WriteLog("Processing completed.");
            }
            catch (Exception ex)
            {
                WriteLog($"Exception: {ex?.Message}: {ex?.InnerException?.Message}");
            }
        }

        #endregion
    }
}