using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using SendGrid;
using SendGrid.Helpers.Mail;
using System.Net.Mail;
using Microsoft.Extensions.Logging;
using SendGrid.Helpers.Mail.Model;

namespace MRF.Utility
{
    
    public  class EmailConfig : IEmailConfig,  IDisposable
    {
        private readonly IConfiguration _configuration;
        private readonly ISendGridClient _sendGridClient;
        private readonly string FromEmail;
        private readonly string SenderName;
        private readonly string smtpServer;
        private readonly int smtpPort;
        private readonly string senderEmail;
        private readonly SmtpClient smtpClient;
        private readonly ILogger<SmtpEmailService> _logger;
        private readonly bool _useSendGrid;
        public EmailConfig(IConfiguration configuration, ILogger<SmtpEmailService> logger)
        {
            _configuration = configuration;
            string sendGridApiKey = _configuration["SendGridSettings:ApiKey"];
            _sendGridClient = new SendGridClient(sendGridApiKey);
            FromEmail = _configuration["SendGridSettings:FromEmail"];
            SenderName = _configuration["SendGridSettings:SenderName"];
            senderEmail = _configuration["SMTP:senderEmail"];
            smtpServer = _configuration["SMTP:Server"];
            smtpPort = Convert.ToInt32(_configuration["SMTP:Port"]);
            _logger = logger;
            smtpClient = new SmtpClient(smtpServer, smtpPort);
            _useSendGrid =Convert.ToBoolean(_configuration["FeatureToggles:UseSendGrid"]);
        }
        public void SendEmail(string receiverEmail, string subject, string body, string? attachmentPath = null)
        {
             
            try
            {
                if (_useSendGrid)
                {

                    string[] emailTo = receiverEmail.Split(',');

                    for (int i = 0; i < emailTo.Length; i++)
                    {
                        var msg = new SendGridMessage
                        {
                            From = new EmailAddress(FromEmail, SenderName),
                            Subject = subject,
                            HtmlContent = body
                        };

                        msg.AddTo(emailTo[i]);

                        if (attachmentPath != null)
                        {
                            byte[] fileBytes = File.ReadAllBytes(attachmentPath);
                            msg.AddAttachment(Path.GetFileName(attachmentPath), Convert.ToBase64String(fileBytes));
                        }

                        var response = _sendGridClient.SendEmailAsync(msg);

                       /* if (!response.IsSuccessStatusCode)
                        {
                            throw new Exception($"Failed to send email. Status code: {response.StatusCode}");
                        }*/
                    }
                }
                else {
                    using (MailMessage mailMessage = new MailMessage(senderEmail, receiverEmail, subject, body))
                    {
                        mailMessage.IsBodyHtml = true;
                        /*if (!string.IsNullOrEmpty(attachmentPath))
                        {
                             Attachment attachment = new  Attachment(attachmentPath);
                            mailMessage.Attachments.Add(attachment);
                        }*/
                        smtpClient.Send(mailMessage);
                        _logger.LogInformation("Email sent successfully.");
                    }
                }
                    
                
            }
            catch (SmtpException ex)
            {
                _logger.LogError("SMTP ERROR START");
                _logger.LogError($"message: {ex.Message}");
                _logger.LogError("SMTP ERROR END");
            }
            catch (Exception ex)
            {
                _logger.LogError("EMAIL ERROR START");
                _logger.LogError($"message: {ex.Message}");
                _logger.LogError("SMTP ERROR END");
            }
        }
        public void Dispose()
        {
            smtpClient.Dispose();
        }
    }
}