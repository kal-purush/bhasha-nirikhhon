using System.Xml.Linq;

namespace SystemFinder.Utilities
{
    internal static class XDocumentCreator
    {
        internal static XDocument IsolateSstm(XDocument root)
        {
            var sstm = root.Descendants("Sstm");

            var actual = sstm.Where(e => e.Attribute("z") is not null);
            var reference = sstm.Where(e => e.Attribute("ref") is not null);

            var container = new XElement("Systems");
            foreach (var element in actual)
            {
                container.Add(element);
            }
            foreach (var element in reference)
            {
                container.Add(element);
            }

            var newDoc = new XDocument();
            newDoc.Add(container);

            return newDoc;
        }
    }
}