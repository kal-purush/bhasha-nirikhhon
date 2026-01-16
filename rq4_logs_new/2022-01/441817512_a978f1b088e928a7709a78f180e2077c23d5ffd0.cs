using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Newtonsoft.Json;

namespace AddressBookTrail
{
    class json
    {
        const string JSONPATH = @"C:\Users\anand\Desktop\Bridgelabz\Day10Assigment\AddressBook\AddressBookTrail\AddressBookTrail\json1.json";
        public static void WriteToJson(Dictionary<string, Contact> DictName)
        {
            if (File.Exists(JSONPATH))
            {
                string Json = JsonConvert.SerializeObject(DictName);
                using (StreamWriter sw = new StreamWriter(JSONPATH))
                {
                    sw.WriteLine(Json);
                }
            }
        }





    }
}