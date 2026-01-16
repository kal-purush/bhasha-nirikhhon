using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Serialization;
using XmlCommentsParser;
using XmlCommentsParser.Models;

namespace XmlCommentsParser
{
    public static class XmlComments
    {
        public static DocElement Parse(string file)
        {
            var serializer = new XmlSerializer(typeof(DocElement));
            //serializer.UnknownElement += Serializer_UnknownElement;

            StreamReader reader = new StreamReader(file);
            var docs = (DocElement)serializer.Deserialize(reader);
            var output = new StringWriter();
            reader.Close();

            output.WriteLine(docs.Assembly.Name);
            output.WriteLine(docs.Members.Count());

            docs.Members.ForEach(x => {


                output.WriteLine($"{x.Name}");
                output.WriteLine($"  Example: {x.Example?.InnerXml ?? "null"}");
                x.Exceptions?.ToList().ForEach(p => output.WriteLine($"  Exception {p.Member}: {p.Value}"));
                output.WriteLine($"  Summary: {x.Summary?.InnerXml ?? "null"}");
                x.Parameters?.ToList().ForEach(p => output.WriteLine($"  Parameter {p.Name}: {p.Value}"));
                output.WriteLine($"  Remarks: {x.Remarks?.InnerXml ?? "null"}");
                output.WriteLine($"  Returns: {x.Returns?.InnerXml ?? "null"}");
                x.TypeParameters?.ToList().ForEach(p => output.WriteLine($"  TypeParameter {p.Name}: {p.Value}"));
                output.WriteLine($"  Value: {x.Value?.InnerXml ?? "null"}");
            });

            File.WriteAllText(@"c:\temp\output_file.txt", output.ToString());
            return docs;
        }

        public static IEnumerable<IComment> GetComments(string file)
        {
            var output = new List<IComment>();
            var serializer = new XmlSerializer(typeof(DocElement));
            StreamReader reader = new StreamReader(file);
            var docs = (DocElement)serializer.Deserialize(reader);
            reader.Close();

            docs.Members.ForEach(x => {
                var name = x.Name.Split(':');
                output.Add( new Comment { Name = name[1], SourceObjectType = GetSourceObjectType(name[0]) });
            });

            return output;
        }

        private static SourceObjectType GetSourceObjectType(string typeAbbreviation)
        {
            switch (typeAbbreviation)
            {
                case "T":
                    return SourceObjectType.Type;
                default:
                    return SourceObjectType.Unknown;
            }
        }
    }
}