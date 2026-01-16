
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Xml.Serialization;

namespace FileIOOperations
{
    //Class Containing XMLElements
    public class XMLElements
    {
        public string Name;
        public DateTime dateTime;
    }
    class XMLOperations
    {
        //Converting from object to XML Format
        public static void SerializeXML()
        {
            //XMLSerializer: Serialize and Deserialize objects and XML document
            XmlSerializer serializer = new XmlSerializer(typeof(XMLElements));
            FileStream file = new FileStream(@"C:\Users\Radhika\source\repos\FileIOOperations\FileIOOperations\XMLData.txt", FileMode.Create);
            //Create object for XMLElement Class
            XMLElements xMLElements = new XMLElements();
            //Assign values for class variables
            xMLElements.Name = "Ash";
            DateTime dateTime = new DateTime(2021, 7, 26);
            xMLElements.dateTime = dateTime;
            //Serialize XMLElement Class
            serializer.Serialize(file, xMLElements);
        }

        //Converting from XML Format to Object
        public static void DeSerializeXML()
        {
            //XMLSerializer: Serialize and Deserialize objects and XML document
            XmlSerializer serializer = new XmlSerializer(typeof(XMLElements));
            //Open File containing XML
            using (FileStream fileStream = new FileStream(@"C:\Users\Radhika\source\repos\FileIOOperations\FileIOOperations\XMLData.txt", FileMode.Open))
            {
                //DEserialize XML format to Object
                XMLElements result = (XMLElements)serializer.Deserialize(fileStream);
                Console.WriteLine("-----After Deserialization------\n Name: {0} \t DateTime: {1}", result.Name, result.dateTime);
            }

        }

    }
}