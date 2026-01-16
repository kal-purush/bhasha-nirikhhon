using System;
using System.Collections.Generic;
using System.IO;
using System.Xml.Serialization;

namespace RealworldIntFINAL;
// Main 2 Guides: https://stackoverflow.com/questions/3671259/how-to-xml-serialize-a-dictionary MAIN: http://web.archive.org/web/20100703052446/http://blogs.msdn.com/b/psheill/archive/2005/04/09/406823.aspx

public class StockDictionaryEntry
{
    public string Symbol { get; set; }
    public decimal Price { get; set; }
}