using System;
using System.Net;
using System.Net.Mail;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace LunaBank.Api.Services
{
    public class EmailService : IEmailService
    {
        private readonly IConfigurationSection _emailConfig;
        private readonly ILogger<EmailService> _logger;
        public EmailService(IConfiguration config, ILogger<EmailService> logger)
        {
           _emailConfig = config.GetSection("Email");
           _logger = logger;
        }
        public void SendMail(string email, string message, string subject)
        {
            try
            {
                // Credentials
                var credentials = new NetworkCredential(_emailConfig.GetValue<string>("Address"),
                                                        _emailConfig.GetValue<string>("Password"));

                // Mail message
                var mail = new MailMessage()
                {
                    From    = new MailAddress("noreply@lunabank.com"),
                    Subject = subject,
                    Body    = message
                };

                mail.IsBodyHtml = true;

                mail.To.Add(new MailAddress(email));

                // Smtp client
                var client = new SmtpClient()
                {
                    Port                  = 587,
                    DeliveryMethod        = SmtpDeliveryMethod.Network,
                    UseDefaultCredentials = false,
                    Host                  = "smtp.gmail.com",
                    EnableSsl             = true,
                    Credentials           = credentials
                };

                client.Send(mail);
            }
            catch (Exception e)
            {
                _logger.LogInformation($"Something went wrong, unable to send email to {email} on {DateTime.Now}");
            }
        }
    }
}