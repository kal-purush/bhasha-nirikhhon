using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FileIOOperations
{
    public class FileOperations
    {
        /// <summary>
        /// file exists or not
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static bool FilExists(string path)
        {
            try
            {
                if(File.Exists(path))
                {
                    Console.WriteLine("File exists");
                    return true;
                }
                else
                {
                    Console.WriteLine("File not exists");
                    return false;
                }
            }
            catch(FileNotFoundException ex)
            {
                Console.WriteLine(ex.Message);
                return false;
            }
        }

    }
}