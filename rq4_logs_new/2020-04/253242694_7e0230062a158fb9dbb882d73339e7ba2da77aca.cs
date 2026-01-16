using System;

namespace MAVN.Service.SmartVouchers.Client.Models.Requests
{
    /// <summary>
    /// Base request model for voucher campaigns
    /// </summary>
    public class VoucherCampaignBase
    {
        /// <summary>Voucher campaign name</summary>
        public string Name { get; set; }

        /// <summary>Voucher campaign description</summary>
        public string Description { get; set; }
        /// <summary>Total vouchers count</summary>
        public int VouchersTotalCount { get; set; }

        /// <summary>Voucher price</summary>
        public decimal VoucherPrice { get; set; }

        /// <summary>Voucher price currency</summary>
        public string Currency { get; set; }

        /// <summary>Voucher campaign issuer</summary>
        public string PartnerId { get; set; }

        /// <summary>Voucher campaign start date</summary>
        public DateTime FromDate { get; set; }

        /// <summary>Voucher campaign end date</summary>
        public DateTime? ToDate { get; set; }
    }
}