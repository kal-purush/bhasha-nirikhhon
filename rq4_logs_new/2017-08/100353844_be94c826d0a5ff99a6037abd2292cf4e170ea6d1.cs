using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace Gynac.Models
{
    public class UserLogModel
    {
        public int UserId { get; set; }
        public string UserIpAddress { get; set; }
        public string UserAgent { get; set; }
    }
}