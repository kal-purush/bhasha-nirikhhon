namespace MAVN.Service.SmartVouchers.Client.Models.Responses
{
    /// <summary>
    /// Response model
    /// </summary>
    public class PublishedAndActiveCampaignsVouchersCountResponse
    {
        /// <summary>
        /// Total count of vouchers in the system which are for campaigns with status published
        /// </summary>
        public int PublishedCampaignsVouchersTotalCount { get; set; }
        /// <summary>
        /// Total count of vouchers in the system which are for campaigns with status published and are currently active
        /// </summary>
        public int ActiveCampaignsVouchersTotalCount { get; set; }
    }
}