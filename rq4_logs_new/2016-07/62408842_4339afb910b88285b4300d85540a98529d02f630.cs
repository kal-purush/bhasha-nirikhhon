using System;
using System.Net.Mail;
using System.Threading.Tasks;
using Microsoft.AspNet.Identity;


namespace PlaceSharer.BLL.Services
{
    class EmailService : IIdentityMessageService
    {
        public Task SendAsync(IdentityMessage message)
        {
            string fromEmail = "placesharer@gmail.com";
            string fromEmailPass = "pisarenko";

            SmtpClient smtpClient = new SmtpClient("smtp.gmail.com", 587);

            smtpClient.DeliveryMethod = SmtpDeliveryMethod.Network;
            smtpClient.UseDefaultCredentials = false;
            smtpClient.Credentials = new System.Net.NetworkCredential(fromEmail, fromEmailPass);
            smtpClient.EnableSsl = true;

            var mail = new MailMessage(fromEmail, message.Destination);
            mail.Subject = message.Subject;
            mail.Body = message.Body;
            mail.IsBodyHtml = true;

            return smtpClient.SendMailAsync(mail);
        }
    }
}