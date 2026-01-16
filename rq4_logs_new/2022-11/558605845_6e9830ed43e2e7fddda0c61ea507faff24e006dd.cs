using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using BusinessLogic;

namespace Domain
{
    [DataContract]
    public class Players
    {
        public Players()
        {
            this.user = new Users();
        }
        [DataMember]
        public int idPlayer { get; set; }
        [DataMember]
        public string playerName { get; set; }
        [DataMember]
        public string playerDescription { get; set; }
        [DataMember]
        public int playerLevel { get; set; }
        [DataMember]
        public string playerRank { get; set; }
        [DataMember]
        public Domain.Users user { get; set; }
    }
}