using System.Runtime.Serialization;

namespace SimpleSmtpInterceptor.Data.Models
{
    [DataContract]
    public class EmailHeader
    {
        [DataMember]
        public string Subject { get; set; }

        [DataMember]
        public string From { get; set; }

        [DataMember]
        public string To { get; set; }

        [DataMember]
        public string MimeVersion { get; set; }

        [DataMember]
        public string Date { get; set; }

        [DataMember]
        public string ContentType { get; set; }

        [DataMember]
        public string ContentTransferEncoding { get; set; }
    }
}