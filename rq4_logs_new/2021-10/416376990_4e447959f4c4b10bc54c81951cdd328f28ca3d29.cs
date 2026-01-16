using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PopLibrary.SqlModels
{
    public class Payment
    {
        public int? Id { get; set; }

        public int? AuctionId { get; set; }

        public int? CustomerId { get; set; }

        public bool? Complete { get; set; }

        public string? StripeInvoiceItemId { get; set; }

        public string? StripeInvoiceId { get; set; }

        public DateTime? Created { get; set; }

        public decimal? Amount { get; set; }

        public string? Description { get; set; }

    }
}