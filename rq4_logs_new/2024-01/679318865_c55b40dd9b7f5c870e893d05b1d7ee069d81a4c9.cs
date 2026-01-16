using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Mail;

namespace MRF.Utility
{
    public class SmtpEmailService : ISmtpEmailService, IDisposable
    {
        private readonly IConfiguration _configuration;
        private readonly ILogger<SmtpEmailService> _logger;
        private readonly string senderEmail;
        private readonly string password;
        private readonly string smtpServer;
        private readonly int smtpPort;
        private readonly SmtpClient smtpClient;

        public SmtpEmailService(IConfiguration configuration, ILogger<SmtpEmailService> logger)
        {
            _configuration = configuration;
            senderEmail = _configuration["SendGridSettings:senderEmail"];
            password = _configuration["SendGridSettings:password"];
            smtpServer = _configuration["SendGridSettings:smtpServer"];
            smtpPort = Convert.ToInt32(_configuration["SendGridSettings:smtpPort"]);
            _logger = logger;
            smtpClient = new SmtpClient(smtpServer, smtpPort);
            smtpClient.Credentials = new NetworkCredential(senderEmail, password);
            smtpClient.EnableSsl = true;
        }

        public void SendEmail(string receiverEmail, string subject, string body, string? attachmentPath = null)
        {
            try
            {
                using (MailMessage mailMessage = new MailMessage(senderEmail, receiverEmail, subject, body))
                {
                    if (!string.IsNullOrEmpty(attachmentPath))
                    {
                        Attachment attachment = new Attachment(attachmentPath);
                        mailMessage.Attachments.Add(attachment);
                    }
                    smtpClient.Send(mailMessage);
                    _logger.LogInformation("Email sent successfully.");
                }
            }
            catch (SmtpException ex)
            {
                _logger.LogError($"Failed to send email. SMTP Error message: {ex.Message}");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Failed to send email. Error message: {ex.Message}");
            }
        }

        public void Dispose()
        {
            smtpClient.Dispose();
        }
    }
}