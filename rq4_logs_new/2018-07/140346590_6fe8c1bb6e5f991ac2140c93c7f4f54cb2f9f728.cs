using System;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Serialization;
using TestXml.XmlCodeComments;

namespace TestXml
{
    class Program
    {
        static void Main(string[] args)
        {
            //string path = @"c:\temp\serialized_xml_file.xml";
            string path = @"c:\temp\Health.xml";

            var serializer = new XmlSerializer(typeof(XmlCodeCommentDoc));
            serializer.UnknownElement += Serializer_UnknownElement;

            StreamReader reader = new StreamReader(path);
            var docs = (XmlCodeCommentDoc)serializer.Deserialize(reader);
            var output = new StringWriter();
            reader.Close();

            output.WriteLine(docs.Assembly.Name);
            output.WriteLine(docs.Members.Count());
            docs.Members.ForEach(x => {
                output.WriteLine($"{x.Name}");
                output.WriteLine($"  Example: {x.Example?.Value ?? "null"}");
                x.Exceptions?.ToList().ForEach(p => output.WriteLine($"  Exception {p.Member}: {p.Value}"));
                output.WriteLine($"  Summary: {x.Summary?.Value ?? "null"}");
                x.Parameters?.ToList().ForEach(p => output.WriteLine($"  Parameter {p.Name}: {p.Value}"));
                output.WriteLine($"  Remarks: {x.Remarks ?? "null"}");
                output.WriteLine($"  Returns: {x.Returns?.Value ?? "null"}");
                x.TypeParameters?.ToList().ForEach(p => output.WriteLine($"  TypeParameter {p.Name}: {p.Value}"));
                output.WriteLine($"  Value: {x.Value?.Value ?? "null"}");
            });

            File.WriteAllText(@"c:\temp\output_file.txt", output.ToString());
        }

        private static void Serializer_UnknownElement(object sender, XmlElementEventArgs e)
        {
            if (sender is XmlCodeCommentFormattableElement) 
            {
                var element = (XmlCodeCommentSummary)e.ObjectBeingDeserialized;
                element.Value = e.Element.InnerXml;
            }
            else if (sender is XmlCodeCommentParameter)
            {
                var element = (XmlCodeCommentParameter)e.ObjectBeingDeserialized;
                element.Value = e.Element.InnerXml;
            }
        }
    }
}