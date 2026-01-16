using MailKit.Net.Smtp;
using Microsoft.Extensions.Options;
using MimeKit;
using System;
using MailKit.Security;
using Models;

namespace ApiManager
{
    public class SmtpOptions
    {
        public string SmtpServer { get; set; }
        public int SmtpPortNumber { get; set; }
        public string FromAdressTitle { get; set; }
        public string FromAddress { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public string ClientUri { get; set; }
    }

    public interface IEmailProvider
    {
        void SendResetPwdEmail(User user);
    }

    public class EmailProvider : IEmailProvider
    {
        private readonly SmtpOptions _options;

        public EmailProvider(IOptions<SmtpOptions> optionsAccessor)
        {
            _options = optionsAccessor.Value;
        }

        public void SendResetPwdEmail(User user)
        {
            try
            {
                var name = string.Format("{0} {1}", user.Name, user.Lastname);
                string ToAddress = user.Email;
                string ToAdressTitle = name;
                string Subject = "EasyPay Reset Password Request";
                string BodyContent = string.Format("Hello {0}. Please follow this link in order to reset your account password. {1}resetpwd?token={2}",
                    name, _options.ClientUri, user.VerificationToken);

                var mimeMessage = new MimeMessage();
                mimeMessage.From.Add(new MailboxAddress(_options.FromAdressTitle, _options.FromAddress));
                mimeMessage.To.Add(new MailboxAddress(ToAdressTitle, ToAddress));
                mimeMessage.Subject = Subject;
                mimeMessage.Body = new TextPart("plain")
                {
                    Text = BodyContent
                };

                using (var client = new SmtpClient())
                {
                    client.AuthenticationMechanisms.Remove("XOAUTH2");
                    client.Connect(_options.SmtpServer, _options.SmtpPortNumber, SecureSocketOptions.StartTls);
                    client.Authenticate(_options.Username, _options.Password);
                    client.Send(mimeMessage);
                    client.Disconnect(true);
                }
            }
            catch (Exception ex)
            {

                throw ex;
            }
        }
    }
}