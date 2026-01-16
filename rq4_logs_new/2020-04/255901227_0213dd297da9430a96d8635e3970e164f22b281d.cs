using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using SendGrid;
using SendGrid.Helpers.Mail;

namespace NeedyBuddy.Services
{
    public class SendMail
    {
        public Task MailSend(string Email, string apiKey)
        {
            var client = new SendGridClient(apiKey);
            var from = new EmailAddress("c2chack20@gmail.com", "NeedyBuddy Admin");
            var subject = "Successfully Registered....";
            var to = new EmailAddress(Email, "Admin NeedyBuddy");
            string[] mailname = Email.Split("@");
            string name = mailname[0];
            var plainTextContent = "Hi "+ name + "</br></br> You have successfully registered with NeedyBuddy Application.";
            var htmlContent = "Hi " + name + "</br></br> You have successfully registered with NeedyBuddy Application.";
            var msg = MailHelper.CreateSingleEmail(from, to, subject, plainTextContent, htmlContent);
            var response = client.SendEmailAsync(msg);
            return response;
        }
    }
}