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
public class StockDictionarySerializer
{
    public void Serialize(TextWriter writer, StockDictionary dictionary)
    {
        List<StockDictionaryEntry> entries = new List<StockDictionaryEntry>(dictionary.Stocks.Count);
        foreach (var key in dictionary.Stocks)
        {
            entries.Add(new StockDictionaryEntry { Symbol = key.Key, Price = key.Value });
        }
        XmlSerializer serializer = new XmlSerializer(typeof(List<StockDictionaryEntry>));
        serializer.Serialize(writer, entries);
    }

    public static StockDictionary Deserialize(TextReader reader)
    {
        XmlSerializer serializer = new XmlSerializer(typeof(List<StockDictionaryEntry>));
        List<StockDictionaryEntry> list = (List<StockDictionaryEntry>)serializer.Deserialize(reader);

        StockDictionary dictionary = new StockDictionary();
        foreach (StockDictionaryEntry entry in list)
        {
            dictionary.Stocks[entry.Symbol] = entry.Price;
        }

        return dictionary;
    }
}