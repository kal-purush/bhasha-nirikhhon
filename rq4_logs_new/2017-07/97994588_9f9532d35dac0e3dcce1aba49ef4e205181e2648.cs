using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Schema;
using System.Xml.Serialization;

namespace ConsoleApplication1.site.DataStructs
{
    public class Page : IXmlSerializable, IEquatable<Page>
    {
        public Uri Url { get; set; }
        public HttpStatusCode StatusCode { get; set; }
        public List<TypedUrl> UrlsOnPage { get; set; } = new List<TypedUrl>();

        public XmlSchema GetSchema() => null;

        public void ReadXml(XmlReader reader)
        {
            reader.MoveToFirstAttribute();
            this.Url = new Uri(reader.Value, UriKind.Absolute);

            reader.MoveToNextAttribute();
            this.StatusCode = (HttpStatusCode)int.Parse(reader.Value);

            reader.MoveToElement();

            if (!reader.IsEmptyElement)
            {
                reader.ReadStartElement();

                while (reader.NodeType != XmlNodeType.EndElement)
                {
                    reader.MoveToFirstAttribute();
                    TypedUrl urlOnPage = new TypedUrl();
                    urlOnPage.Url = new Uri(reader.Value, UriKind.Absolute);

                    reader.MoveToNextAttribute();
                    urlOnPage.UrlType = (Robot.LinkType)Enum.Parse(typeof(Robot.LinkType), reader.Value);

                    this.UrlsOnPage.Add(urlOnPage);

                    reader.MoveToElement();
                    reader.Read();
                }
            }
            reader.Read();
        }

        public void WriteXml(XmlWriter writer)
        {
            writer.WriteAttributeString("Url", this.Url.AbsoluteUri);
            writer.WriteAttributeString("StatusCode", ((int)this.StatusCode).ToString());
            

            foreach (TypedUrl typedUrl in this.UrlsOnPage)
            {
                writer.WriteStartElement("UrlOnPage");

                writer.WriteAttributeString("AbsoluteUrl", typedUrl.Url.AbsoluteUri);
                writer.WriteAttributeString("LinkType", typedUrl.UrlType.ToString());

                writer.WriteEndElement();
            }                       
        }

        public bool Equals(Page other)
        {
            return this.Url == other.Url;
        }
    }
}