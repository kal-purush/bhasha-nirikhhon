using System;
using OpenDataImport.models;
using System.Collections;
using System.Linq;
using System.Xml.Linq;

namespace OpenDataImport
{
    class Program
    {
        static void Main(string[] args)
        {
            //Console.WriteLine("Hello World!");
            var nodes = findOpenData();
            ShowOpenData(nodes);
            Console.ReadKey();
        }

        static List<OpenData> findOpendata()
        {
            List<OpenData> result = new List<OpenData>();
            var xml = XElement.Load(@"C:\Users\user\Desktop\scenic_spot_C_f.xml");
            var nodes = xml.Descendants("node").ToList();

            for (var i = 0; i < nodes.Count; i++)
            {
                var node = nodes[i];
                OpenData item = new OpenData();


            }
        }

    }
}