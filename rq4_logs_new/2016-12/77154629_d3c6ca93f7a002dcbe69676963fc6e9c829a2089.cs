using System.Collections.Generic;
using System.Xml.Serialization;

namespace DotNetCasClient.Validation.Schema.Cas20
{
    public class Attribute
    {
        [XmlAttribute("name")]
        public string Name { get; set; }
        [XmlAttribute("value")]
        public string Value { get; set; }
    }
}