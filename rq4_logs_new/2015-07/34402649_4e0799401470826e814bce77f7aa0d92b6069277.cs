using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Xml;
using System.Xml.Serialization;

namespace SupplyChainManagement.Models.Xml
{
    public class WaitingList
    {
        [XmlAttribute("period")]
        public int Period;

        [XmlAttribute("order")]
        public int Order;

        [XmlAttribute("firstbatch")]
        public int FirstBatch;

        [XmlAttribute("lastbatch")]
        public int LastBatch;

        [XmlAttribute("item")]
        public int Item;

        [XmlAttribute("amount")]
        public int Amount;
    }
}