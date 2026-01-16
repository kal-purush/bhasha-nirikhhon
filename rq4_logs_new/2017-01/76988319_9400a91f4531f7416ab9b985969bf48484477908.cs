using System.Net;

namespace GoogleRecaptcha.Models
{
    public class GoogleRecaptchaResponse
    {
        public HttpStatusCode StatusCode { get; set; }

        public string ReasonPhase { get; set; }

        public GoogleRecaptchaResponseContent ResponseContent {get; set; }
        
        public bool IsSuccessStatusCode { get; set; }
    }
}