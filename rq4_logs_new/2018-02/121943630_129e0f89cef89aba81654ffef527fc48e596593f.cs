using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AdpterApi.Configuration
{
    public class MailerSettings
    {
        public string FromAddress { get; set; }
        public string ToAddress { get; set; }
        public string MailServiceUrl { get; set; }
    }
}