using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Xml.Linq;

namespace DEV_8
{
    public class XmlParser
    {
        public string XmlFile { get; set; }

        public XmlParser(string addressXml)
        {
            this.XmlFile = addressXml;
        }

        public List<Car> parseInListOfCars()
        {
            XDocument xdoc = XDocument.Load(XmlFile);
            List<Car> result = xdoc.Element("machines")?.Elements("car").Select
            (x => new Car(x.Element("brand").Value, x.Element("model").Value,
                Int32.Parse(x.Element("numberOfCars").Value),
                Decimal.Parse(x.Element("price").Value,CultureInfo.InvariantCulture))).ToList();

            return result;
        }
        
        public List<Truck> parseInListOfTrucks()
        {
            XDocument xdoc = XDocument.Load(XmlFile);
            List<Truck> result = xdoc.Element("machines")?.Elements("truck").Select
            (x => new Truck(x.Element("brand").Value, x.Element("model").Value,
                Int32.Parse(x.Element("numberOfCars").Value),
                Decimal.Parse(x.Element("price").Value,CultureInfo.InvariantCulture))).ToList();

            return result;
        }
    }
}