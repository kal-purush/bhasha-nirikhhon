using System;
using System.Runtime.Serialization;

namespace WxAPP
{
    [DataContract(Name="wxResults")]
    public class WxResults
    {
        [DataMember(Name="list")]
        public string List;
    }
}