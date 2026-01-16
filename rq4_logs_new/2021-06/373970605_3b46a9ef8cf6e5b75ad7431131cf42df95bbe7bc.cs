namespace IdentityServer.Controllers.Models
{
    public class AuthRequest
    {
        public string scope { get; set; }
        public string client_id { get; set; }
        public string grant_type { get; set; }
        public string client_secret { get; set; }
    }
}