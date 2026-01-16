using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Application.DTOs
{
    public record PaymentRequest
    {
        public string Email { get; set; } = default!;
        public decimal Amount { get; set; }        
        public string Reference { get; set; } = default!;
        public string Currency { get; set; } = default!;
        public string Callback_url { get; set; } = default!;
    }

    public record InitiateResponse
    {
        public bool Status { get; set; }
        public string Message { get; set; } = default!;
        public UrlDetails? Data { get; set; }
    }

    public record UrlDetails
    {
        public string AuthorizationUrl { get; set; } = default!;
        public string AccessCode { get; set; } = default!;
        public string Reference { get; set; } = default!;
    }
}