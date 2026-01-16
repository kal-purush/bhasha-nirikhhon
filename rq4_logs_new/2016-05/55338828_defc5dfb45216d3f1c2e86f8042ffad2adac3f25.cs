using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace K4_Projekt
{
    class eventLog
    {
        private static eventLog log = null;
        private static bool logFile = false;
        private static List<String> puffer;

        public static eventLog getLog()
        {

            if (log == null)
            {
                puffer = new List<String>();
                log = new eventLog();
            }
            return log;
        }
        public void addLog(string _date, string _time, string _patient, string _triageNr, string _action)
        {
            puffer.Add(_date + ";" + _time + ";" + _patient + ";" + _triageNr + ";" + _action);//write to file
        }
        public void writeToFile(String _file)
        {
            if (!logFile)
            {
                File.WriteAllText(_file, "");
                logFile = true;
            }
            System.IO.StreamWriter file = new System.IO.StreamWriter(_file, true);
            foreach (String s in puffer)
            {
                file.WriteLine(s);
            }
            file.Close();
        }


        public List<string> fromFileToList(String _file, String delimiter)
        {
            List<string> log = new List<string>();
            if (logFile)//checking if file exists
            {
                //Open file Stream
                System.IO.StreamReader file = new System.IO.StreamReader(_file);
                string support;
                while ((support = file.ReadLine()) != null)//while not end of file
                {
                    if (support.Length != 0)//checking if file is not empty, only for important for the first iteration
                    {
                        log.Add(support.Replace(";", delimiter));
                    }
                    else
                    {
                        throw new Exception("File empty");
                    }

                }


                file.Close();
            }
            else
            {
                throw new Exception("File not found");
            }

            return log;


        }
        public List<string> fromFileToConsole(String _file, String delimiter)
        {
            List<string> log = new List<string>();
            if (logFile)//checking if file exists
            {
                //Open file Stream
                System.IO.StreamReader file = new System.IO.StreamReader(_file);
                string support;
                while ((support = file.ReadLine()) != null)//while not end of file
                {
                    if (support.Length != 0)//checking if file is not empty, only for important for the first iteration
                    {
                        Console.WriteLine(support.Replace(";", delimiter));
                    }
                    else
                    {
                        throw new Exception("File empty");
                    }

                }


                file.Close();
            }
            else
            {
                throw new Exception("File not found");
            }

            return log;


        }

    }
}