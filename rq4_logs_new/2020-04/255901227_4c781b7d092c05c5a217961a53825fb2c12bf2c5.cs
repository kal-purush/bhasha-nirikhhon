using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SendGrid;
using SendGrid.Helpers.Mail;

namespace NeedyBuddy.Models
{
    public class MailTemplate
    {
        public Task MailSend(string senderEmail, string recipientEmail, string mailSubject, string mailBody, string apiKey)
        {
            var client = new SendGridClient(apiKey);
            var from = new EmailAddress(senderEmail, "NeedyBuddy Admin");
            var subject = mailSubject;
            var to = new EmailAddress(recipientEmail, "Admin NeedyBuddy");
            string[] mailname = recipientEmail.Split("@");
            string name = mailname[0];
            var plainTextContent = mailBody;
            var htmlContent = mailBody;
            var msg = MailHelper.CreateSingleEmail(from, to, subject, plainTextContent, htmlContent);
            var response = client.SendEmailAsync(msg);
            return response;
        }
    }
}