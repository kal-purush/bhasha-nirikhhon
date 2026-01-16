using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace DeeEnDee
{
    class Program
    {
        static int Main(string[] args)
        {
            XmlDocument doc = new XmlDocument();
            //doc.PreserveWhitespace = true;
            try { doc.Load("Monster_List.xml"); }
            catch (System.IO.FileNotFoundException)
            {
                Console.WriteLine("Failed to load Monster_List.xml");
                return 0;
            }

            XmlElement elem = doc.GetElementById("Ape");
            Console.WriteLine(elem.OuterXml);

            return 0;
        }
    }
}