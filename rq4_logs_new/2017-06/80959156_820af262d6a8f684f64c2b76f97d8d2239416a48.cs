using System.IO;
using System;
using System.Collections.Generic;
namespace FinalProject
{
    /// <summary>
    /// Read text files with data
    /// </summary>
    class FileReader
    {
        private char[] separators = { ';' };

        /// <summary>
        /// Read text file and return list of arrays, with contais splitted strings
        /// </summary>
        /// <param name="fileName">name of file to read</param>
        /// <returns> list of string arrays</returns>
        public List<string[]> GetDataFrom(string fileName)
        {
            List<string[]> allData = new List<string[]>();
            using (StreamReader reader = new StreamReader(fileName))
            {
                string line = string.Empty;
                while ((line = reader.ReadLine()) != null)
                {
                    allData.Add(line.Split(separators));
                }
            }
            return allData;
        }
    }
}