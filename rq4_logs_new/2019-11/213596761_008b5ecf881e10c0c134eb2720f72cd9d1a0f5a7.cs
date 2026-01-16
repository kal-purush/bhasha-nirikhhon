using MailKit.Net.Smtp;
using Microsoft.Extensions.Configuration;
using MimeKit;

namespace servicer.API.Services
{
    public class EmailService : IEmailService
    {
        private IConfiguration _configuration;

        public EmailService(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void SendEmailMessage(string toAddress, string subject, string messageHtml, string messageTxt)
        {
            MimeMessage emailMessage = new MimeMessage();

            MailboxAddress from = new MailboxAddress("servicer", "servicer.smtp@gmail.com");
            emailMessage.From.Add(from);

            MailboxAddress to = new MailboxAddress("User", toAddress);
            emailMessage.To.Add(to);

            emailMessage.Subject = subject;

            BodyBuilder bodyBuilder = new BodyBuilder();
            bodyBuilder.HtmlBody = messageHtml;
            bodyBuilder.TextBody = messageTxt;

            emailMessage.Body = bodyBuilder.ToMessageBody();

            SmtpClient client = new SmtpClient();
            client.Connect(_configuration.GetSection("EmailConfiguration:EmailAddress").Value, 587, false);
            client.Authenticate(_configuration.GetSection("EmailConfiguration:Login").Value, _configuration.GetSection("EmailConfiguration:Password").Value);

            client.Send(emailMessage);
            client.Disconnect(true);
            client.Dispose();
        }
    }
}