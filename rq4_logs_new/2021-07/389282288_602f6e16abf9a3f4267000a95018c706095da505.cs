
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using CsvHelper;

namespace FileIOOperations
{
    class CsvOperations
    {
        public static string importFilePath = @"C:\Users\Radhika\source\repos\FileIOOperations\FileIOOperations\csvdata.csv";
        //Read Content of CSV File and Print
        public static void CsvDeSerailize()
        {
            //Initialization

            using var reader = new StreamReader(importFilePath);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
            var result = csv.GetRecords<Person>().ToList();
            foreach (Person m in result)
            {
                Console.WriteLine("Name: {0} \t Email: {1} \t Phone Number: {2} \t Country: {3}", m.name, m.email, m.phoneNum, m.country);
            }
        }

        public static void CsvSerailize()
        {
            string exportfile = @"C:\Users\Radhika\source\repos\FileIOOperations\FileIOOperations\csvdata.csv";
            using var writer = new StreamWriter(exportfile);

            using var csvWrite = new CsvWriter(writer, CultureInfo.InvariantCulture);
            
            List<Person> data = new List<Person>
            {
            new Person("Sumathi","suma@yahoo.com",8964856932,"India") };
            csvWrite.WriteRecords(data);
            csvWrite.NextRecordAsync();
        }
    }
}
