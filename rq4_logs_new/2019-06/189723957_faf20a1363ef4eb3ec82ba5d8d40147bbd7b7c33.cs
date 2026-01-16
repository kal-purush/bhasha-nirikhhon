using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UtilityLibraries
{
    public static class Authentication
    {
        public static bool Login(string path)
        {
            //Если файл существует
            if (File.Exists(path))
            {
                //Считываем все строки в файл 
                string[] fileArray = File.ReadAllText(path).Split(' ');
                return (fileArray[0] == "root" && fileArray[1] == "GeekBrains") ? true : false;
            }
            else
            {
                Console.WriteLine("Error load file");
                return false;
            }
            
        }
    }
}