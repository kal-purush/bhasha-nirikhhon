namespace Lykke.Service.PayInvoice.Client.Models.Employee
{
    /// <summary>
    /// Mark employee deleted request
    /// </summary>
    public class MarkEmployeeDeletedRequest
    {
        /// <summary>
        /// Gets or sets a merchant id.
        /// </summary>
        public string MerchantId { get; set; }

        /// <summary>
        /// Gets or sets an employee id.
        /// </summary>
        public string EmployeeId { get; set; }
    }
}