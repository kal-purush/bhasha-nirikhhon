using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ArvidsonFoto_LogReader.Model;

namespace ArvidsonFoto_LogReader.Data
{
    public class AppLogReader
    {
        string folderDataPath = @".\logs\"; //Pekar på mappen där loggfilen ligger

        public List<LogPost> ReadData(string appLogFile)
        {
            string inputDataPath = folderDataPath + appLogFile; //Sätter sökvägen till filen.
            List<string> dataList = new List<string>();
            List<LogPost> returnList = new List<LogPost>();
            try { //Försöker öppna filen och läsa den...
                using (StreamReader reader = File.OpenText(inputDataPath))//Läser filen, eller ger null om ingen fil finns
                {
                    string line = "";
                    while ((line = reader.ReadLine()) != null) //Loopar igenom alla rader (line) i filen
                    {
                        dataList.Add(line);
                    }
                }
            }
            catch (Exception ex)
            {
                dataList.Add(ex.Message);
            }
            
            for (int i = dataList.Count; i > 0; i--)
            {
                string logRow = dataList[i - 1];
                string[] splittedRow = logRow.Split(" ");

                string logIPadress = splittedRow[0];

                string loggerDate = splittedRow[3] + " " + splittedRow[4];
                loggerDate = loggerDate.Replace("[", "");
                loggerDate = loggerDate.Replace("]", "");
                //string logDate = Convert.ToDateTime(loggerDate);
                
                string logHTTPmethod = splittedRow[5];
                logHTTPmethod = logHTTPmethod.Replace("\"", "");

                string logUrlPath = splittedRow[6];
                logUrlPath = logUrlPath.Replace("\"", "");

                string logErrorCode = splittedRow[7];
                string logTimeToLoad = splittedRow[8];
                string logRequestFrom = splittedRow[9];
                string logBrowser = splittedRow[10];

                LogPost logPostRow = new LogPost()
                {
                    IPadress = logIPadress,
                    Date = loggerDate,
                    HTTPmethod = logHTTPmethod,
                    UrlPath = logUrlPath,
                    ErrorCode = logErrorCode,
                    TimeToLoad = logTimeToLoad,
                    RequestFrom = logRequestFrom,
                    Browser = logBrowser
                };

                returnList.Add(logPostRow);
            }

            
            return returnList;
        }

        public List<string> ExistingLogFiles()
        {
            List<string> returnList = new List<string>();

            DirectoryInfo di = new DirectoryInfo(folderDataPath);
            var files = di.GetFiles("appLog*.txt");
            foreach (var fileInfo in files)
            {
                string[] splittedFileName = fileInfo.Name.Split("Log"); //[0]app [1]20210207.txt
                string date = splittedFileName[1]; //20210207.txt
                splittedFileName = date.Split("."); //[0]20210207 [1]txt
                date = splittedFileName[0]; //20210207

                returnList.Add(date);
            }

            return returnList;
        }
    }
}