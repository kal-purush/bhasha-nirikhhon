using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using System.Xml.Schema;
using System.Xml.Serialization;

namespace CSharpXmlParser
{
    class Program
    {
        static void Main(string[] args)
        {
            var xmlFile =
                XDocument.Load(
                    @"C:\Users\gulaganjihalli_r1\Desktop\Practice\Core\ConsoleApplication1\ConsoleApplication1\bin\Debug\ConsoleApplication1.XML");
            RootDocument document;
            var serializer = new XmlSerializer(typeof(RootDocument));
            using (var stream = File.OpenRead(@"C:\Users\gulaganjihalli_r1\Desktop\Practice\Core\ConsoleApplication1\ConsoleApplication1\bin\Debug\ConsoleApplication1.XML"))
            {

                document = (RootDocument)serializer.Deserialize(stream);
            }
            if (document != null)
            {
                using (StringWriter textWriter = new StringWriter())
                {
                    serializer.Serialize(textWriter, document);
                    Console.WriteLine(textWriter.ToString());
                }
            }
            Console.ReadLine();
        }
    }
    //<doc>
    //  <assembly>
    //    <name>ConsoleApplication1</name>
    //  </assembly>
    //  <members>
    //    <member name = "T:ConsoleApplication1.Program" >
    //      < summary >
    //            Custom Xml Parse application Header
    //            </summary>
    //    </member>
    //    <member name = "M:ConsoleApplication1.Program.Main(System.String[])" >
    //      < summary >
    //            Test Main method
    //            </summary>
    //      <param name = "args" > Arguments </ param >
    //    </ member >
    //    < member name= "M:ConsoleApplication1.Program.GetUser(System.String,System.String)" >
    //      < summary >
    //            Get User Name
    //            </summary>
    //      <param name = "id" > user id</param>
    //      <param name = "address" > user address</param>
    //      <returns>combined id and address<see cref = "F:ConsoleApplication1.TestCref.Test" /> dfgdfgdfg < see cref= "F:ConsoleApplication1.TestCref.Test" /></ returns >
    //    </ member >
    //    < member name= "T:ConsoleApplication1.TestCref" >
    //      < summary >
    //            Test Class
    //            </summary>
    //    </member>
    //    <member name = "F:ConsoleApplication1.TestCref.Test" >
    //      < summary >
    //            Reference Test
    //            </summary>
    //    </member>
    //  </members>
    //</doc>

    [XmlRoot("doc")]
    public class RootDocument
    {
        [XmlElement("assembly")]
        public ProjectAssembly ProjectAssembly { get; set; }

        [XmlArray("members")]
        [XmlArrayItem("member")]
        public List<Member> Members { get; set; }
    }


    public class ProjectAssembly
    {
        [XmlElement("name")]
        public string Name { get; set; }
    }

    public class Member
    {
        [XmlAttribute("name")]
        public string Name { get; set; }

        [XmlElement("summary")]
        public string Summary { get; set; }

        [XmlElement("param", typeof(Param))]
        public List<Param> Params { get; set; }

        [XmlElement("returns")]
        public Returns Returns { get; set; }
    }

    [XmlRoot("param")]
    public class Param
    {
        [XmlAttribute("name")]
        public string Name { get; set; }
    }

    public class Returns : IXmlSerializable
    {
        public Returns()
        {
            ReturnComments = new Dictionary<int, ReturnComment>();
            Sees = new List<See>();
        }

        [XmlElement("comments")]
        public Dictionary<int, ReturnComment> ReturnComments { get; set; }


        [XmlElement("see", typeof(See))]
        public List<See> Sees { get; set; }

        public XmlSchema GetSchema()
        {
            return null;
        }

        public void ReadXml(XmlReader reader)
        {
            bool endLoop = false;
            reader.MoveToContent();
            reader.ReadStartElement();
            while (!endLoop)
            {
                switch (reader.NodeType)
                {
                    case XmlNodeType.Element:
                        XElement el = XNode.ReadFrom(reader) as XElement;
                        if (el != null && string.Equals(el.Name.ToString(), "see", StringComparison.OrdinalIgnoreCase))
                        {
                            var see = (See)new XmlSerializer(typeof(See)).Deserialize(el.CreateReader());
                            ReturnComments.Add(ReturnComments.Count, new ReturnComment($"{{{Sees.Count}}}", false));
                            Sees.Add(see);
                        }
                        break;
                    case XmlNodeType.EndElement:
                        endLoop = true;
                        break;
                    default:
                        ReturnComments.Add(ReturnComments.Count, new ReturnComment(reader.ReadContentAsString()));
                        break;
                }
            }
        }

        public void WriteXml(XmlWriter writer)
        {
            new XmlSerializer(typeof(List<See>)).Serialize(writer, Sees);
            //TODO: build serializer for dictionary
            //new XmlSerializer(typeof(Dictionary<int, ReturnComment>)).Serialize(writer, ReturnComments);
        }
    }

    [XmlRoot("see")]
    public class See
    {

        [XmlAttribute("cref")]
        public string Cref { get; set; }
    }

    public class ReturnComment
    {
        public ReturnComment(string comment, bool isComment = true)
        {
            Comment = comment;
            IsComment = isComment;
        }
        public bool IsComment { get; set; }

        public string Comment { get; set; }
    }

}