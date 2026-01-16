using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Web;

namespace AAGJKPRTServices.DataContract
{
    [DataContract]
    public class UserInfoDataContract
    {
        [DataMember]
        public string Message;

        [DataMember]
        public bool Status;

        [DataMember]
        public List<userinfo> Data;

        public UserInfoDataContract()
        {
            Data = new List<userinfo>();
        }

    }
    public class userinfo
    {
        public int UserId { get; set; }
        public string fname { get; set; }
        public string mname { get; set; }
        public string lname { get; set; }
        public int age { get; set; }
        public string DOB { get; set; }
    }

}