using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace FileNameChanger
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            string diretoryPath = @"D:\Rovia\20130207\AirLogs\";
            List<string> files = Directory.GetFiles(diretoryPath).ToList();
            int i = 0;
            foreach (var file in files)
            {
                string filedata = File.ReadAllText(file);
                filedata = filedata.Replace("<?xml version=\"1.0\" encoding=\"utf-16\"?>", "").Trim();

                if (!string.IsNullOrWhiteSpace(filedata) && filedata != "\0")
                {
                    string newName = filedata.Substring(0, filedata.IndexOf(" "));

                    newName = newName.Replace('<', ' ').Trim();
                    string checkName = newName + ".xml";
                    i++;
                    File.Move(file, diretoryPath + i + "." + newName + ".xml");
                    //if (File.Exists(diretoryPath + checkName))
                    //{
                    //    i++;
                    //    File.Move(file, diretoryPath+newName + i + ".xml");
                    //}
                    //else
                    //{
                    //    File.Move(file,diretoryPath+newName + ".xml");
                    //}

                }

            }
        }





    }
}