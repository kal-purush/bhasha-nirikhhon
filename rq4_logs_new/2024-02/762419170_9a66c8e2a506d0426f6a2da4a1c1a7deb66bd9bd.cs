using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Mail;
using System.Text;

namespace Common_layer
{
    public class Send
    {
        public string SendMail(string ToEmail,string Token)
        {
            string FromEmail = "namitlodh2605@gmail.com";
            MailMessage Message= new MailMessage(FromEmail, ToEmail);
            string MailBody = "the token for the reset password: " + Token;
            Message.Body = MailBody.ToString();
            Message.BodyEncoding = Encoding.UTF8;
            Message.IsBodyHtml = true;

            SmtpClient smtpClient = new SmtpClient("smtp.gmail.com",587);
            NetworkCredential credential = new 
                NetworkCredential("namitlodh2605@gmail.com", "hbzn lmhz vjip ymlj");

            smtpClient.EnableSsl = true;
            smtpClient.UseDefaultCredentials = true;
            smtpClient.Credentials = credential;

            smtpClient.Send(Message);
            return ToEmail;
        }
    }
}