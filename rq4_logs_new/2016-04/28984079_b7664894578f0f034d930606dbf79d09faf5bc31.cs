using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Xml.Serialization;

namespace Neptuo.WebSite.Models.Projects
{
    [XmlType("Version")]
    [XmlRoot("Version")]
    public class ProjectVersionModel
    {
        public string Name { get; set; }

        [XmlArray("ChangeLog")]
        [XmlArrayItem("Change")]
        public List<string> ChangeLog { get; set; }
    }
}