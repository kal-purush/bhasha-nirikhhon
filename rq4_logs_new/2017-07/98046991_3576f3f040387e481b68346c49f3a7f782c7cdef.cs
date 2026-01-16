using CsvHelper;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using JSONDesralize.Model;
using System.Text;
using System.Threading.Tasks;

namespace JSONDesralize
{
    class AsyncAndWait
    {
        private const string CustomerDetailsFilePath = @".\Content\CustomerData.csv";

        public async static Task<int> ReadFromFile()
        {
            await Task.Delay(1000);
            var csv = new CsvReader(new StreamReader(AsyncAndWait.CustomerDetailsFilePath));
            var records = csv.GetRecords<Customer>().ToList();

            foreach (var record in records)
            {
                Console.WriteLine($"The custome name is {record.Name} and date of bith is {record.DateOfBirth}");
            }

            return 1;
        }
    }
}