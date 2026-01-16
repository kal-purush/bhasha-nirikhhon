namespace EasyTravel.Web
{
    public class SSLCommerzRequest
    {
        public string StoreId { get; set; }
        public string StorePassword { get; set; }
        public string TotalAmount { get; set; }
        public string Currency { get; set; } = "BDT";
        public string TranId { get; set; }
        public string SuccessUrl { get; set; }
        public string FailUrl { get; set; }
        public string CancelUrl { get; set; }
        public string CusName { get; set; }
        public string CusEmail { get; set; }
        public string CusPhone { get; set; }
        public string CusAdd1 { get; set; }
        public string CusCity { get; set; }
        public string CusCountry { get; set; } = "Bangladesh";
    }
}