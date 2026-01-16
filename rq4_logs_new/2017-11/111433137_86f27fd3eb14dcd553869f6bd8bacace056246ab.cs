using System;
using System.IO;
using System.Xml.Linq;
using Newtonsoft.Json;

namespace exercise4
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var jsonFile = File.ReadAllText("E:\\Projects\\Quoc_Lab_5\\employees.json");
                var doc = JsonConvert.DeserializeXmlNode(jsonFile,"root");
                File.WriteAllText("E:\\Projects\\Quoc_Lab_5\\employees.xml", XDocument.Parse(doc.InnerXml).ToString());
                Console.ReadKey();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}