using System.Xml.Serialization;

namespace WiserTaskScheduler.Modules.ImportFiles.Models;

[XmlType("XmlMap")]
public class XmlMapModel 
{
    /// <summary>
    /// Gets or sets the xpath expression to the variable that needs to be imported.
    /// </summary>
    [XmlAttribute("XPathExpression")]
    public string XPathExpression { get; set; }

    /// <summary>
    /// Gets or sets the name to use for the result set.
    /// </summary>
    [XmlAttribute("ResultSetName")]
    public string ResultSetName { get; set; }
}