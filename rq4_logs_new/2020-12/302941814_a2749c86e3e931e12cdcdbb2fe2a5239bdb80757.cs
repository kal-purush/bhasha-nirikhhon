using System;
using System.Collections.Generic;
using System.Text;

namespace SoftPhone.Common.SipClientModels.UserAgents
{
    public class RegistrationUserAgent
    {
        public SipUser SipUser { get; set; }
        public LocalSipUserAgentServer LocalSipUas { get; set; }
        public RemoteSipUserAgentServer RemoteSipUas { get; set; }
    }
}