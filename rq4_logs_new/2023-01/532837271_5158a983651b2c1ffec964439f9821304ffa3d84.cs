namespace TripPlanner.Logic.Services.EmailService.Smtp
{
    public class EmailServiceConfiguration
    {
        public string Host { get; set; } 
        public string Port { get; set; } 
        public bool EnableSsl { get; set; } 
        public string ServerName { get; set; }
        public string Template { get; set; }
        public string Email { get; set; }
        public string Password { get; set; }
    }
}