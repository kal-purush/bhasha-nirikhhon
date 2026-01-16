using CsvHelper;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FileIOOperations
{
    public class JsonOperations
    {
        public static void ReadJsonAndWriteToCSV()
        {

            string jsonFile = @"C:\Users\Radhika\source\repos\FileIOOperations\FileIOOperations\JsonData.json";
            string csvFile = @"C:\Users\Radhika\source\repos\FileIOOperations\FileIOOperations\JsonToCsvData.csv";
            //deserializes the object 
            List<Person> list = JsonConvert.DeserializeObject<List<Person>>(File.ReadAllText(jsonFile));
            Console.WriteLine("****Reading from json file and writing into csv file******");
            using (var writer = new StreamWriter(csvFile))
            using (var csv = new CsvWriter(writer, System.Globalization.CultureInfo.InvariantCulture))
            {
                //writes the record to csv
                csv.WriteRecords(list);
            }
            Console.WriteLine("Data written in csv file successfully");

        }
    }
}