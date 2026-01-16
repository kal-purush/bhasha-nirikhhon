using JetBrains.Annotations;
using MAVN.Service.SmartVouchers.Client.Models.Responses.Enums;

namespace MAVN.Service.SmartVouchers.Client.Models.Responses
{
    /// <summary>
    /// Reserve voucher response model
    /// </summary>
    [PublicAPI]
    public class ReserveVoucherResponse
    {
        /// <summary>Error code</summary>
        public ProcessingVoucherErrorCodes ErrorCode { get; set; }

        /// <summary>Payment url</summary>
        public string PaymentUrl { get; set; }
    }
}