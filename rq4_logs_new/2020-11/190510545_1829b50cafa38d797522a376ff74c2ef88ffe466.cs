using System;

namespace OneBlink.SDK.Model
{

    public class NewFormsAppSendingAddress
    {
        public NewFormsAppSendingAddress(string emailAddress, string emailName = null) {
            this.emailAddress = emailAddress;
            this.emailName = emailName;
        }
        public string emailAddress { get; set; }
        public string emailName { get; set; }
    }
    public class FormsAppSendingAddress
    {
        public string emailAddress { get; set; }
        public string emailName { get; set; }
        public long formsAppId { get; set; }
        public SesVerificationAttributes sesVerificationAttributes { get; set; }
        public DateTime createdAt { get; set; }
        public DateTime updatedAt { get; set; }
    }

    public class SesVerificationAttributes
    {
        string VerificationStatus { get; set; }
    }
}