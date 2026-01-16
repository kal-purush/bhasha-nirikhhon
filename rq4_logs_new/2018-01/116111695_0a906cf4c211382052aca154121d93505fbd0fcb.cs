using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SuperConfigure.Common
{
    public class WriteTextLogs
    {
        #region ### WriteTextLog ###
        public static void WriteTextLog(string path, string text, string suffix = "")
        {
            try
            {
                text = DateTime.Now + " : " + text;

                string fileName = DateTime.Now.Year.ToString()
                    + DateTime.Now.Month.ToString() + DateTime.Now.Day.ToString()
                    + (suffix != "" ? "-" + suffix : "") + ".txt";

                if (!Directory.Exists(path))
                {
                    Directory.CreateDirectory(path);
                }

                if (File.Exists(path + fileName))
                {
                    // Open the file to eidt.
                    using (StreamReader sr = new StreamReader(path + fileName))
                    {
                        string allText = sr.ReadToEnd();
                        text = text + (allText != "" ? Environment.NewLine : "") + allText;
                    }
                }

                // Create a file to write to.
                using (StreamWriter sw = File.CreateText(path + fileName))
                {
                    sw.Write(text);
                }
            }
            catch //(Exception ex)
            {
                //Console.WriteLine("error: " + ex.Message.ToLower());
            }
        }
        #endregion
    }
}