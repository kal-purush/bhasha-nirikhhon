using System.Xml.Serialization;
using SimpleSOAPClient.Models;

namespace XRoad.Domain.Header {
    [XmlRoot(ElementName = "securityServer", Namespace = "http://x-road.eu/xsd/xroad.xsd")]
    public class XRoadSecurityServer : SoapHeader {
        [XmlAttribute(AttributeName = "objectType", Namespace = "http://x-road.eu/xsd/identifiers")]
        public ObjectType ObjectType {
            get => ObjectType.Server;
            // ReSharper disable once ValueParameterNotUsed
            set { }
        }

        [XmlElement(ElementName = "xRoadInstance", IsNullable = false, Namespace = "http://x-road.eu/xsd/identifiers")]
        public string Instance { get; set; }

        [XmlElement(ElementName = "memberClass", IsNullable = false, Namespace = "http://x-road.eu/xsd/identifiers")]
        public string MemberClass { get; set; }

        [XmlElement(ElementName = "memberCode", IsNullable = false, Namespace = "http://x-road.eu/xsd/identifiers")]
        public string MemberCode { get; set; }

        [XmlElement(ElementName = "serverCode", IsNullable = false, Namespace = "http://x-road.eu/xsd/identifiers")]
        public string ServerCode { get; set; }

        public static implicit operator XRoadSecurityServer(SecurityServerIdentifier serverIdentifier) {
            return new XRoadSecurityServer {
                Instance = serverIdentifier.Instance,
                MemberClass = serverIdentifier.MemberClass,
                MemberCode = serverIdentifier.MemberCode,
                ServerCode = serverIdentifier.SecurityServerCode
            };
        }
    }
}