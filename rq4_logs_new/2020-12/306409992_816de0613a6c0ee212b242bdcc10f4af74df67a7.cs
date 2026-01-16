using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Threading.Tasks;

namespace Backend.Service.EmailService
{
    public class TenderService
    {

        public void SendNotification()
        {
            try
            {
                Console.WriteLine("E-mail with information about publishing tender is sending!");
                MailMessage email = new MailMessage();
                SmtpClient smpt = new SmtpClient("smtp.gmail.com");

                email.From = new MailAddress("hospitalssystem@gmail.com");
                email.To.Add("pharmacysistem@gmail.com");
                email.Subject = ("We published tender!");
                email.Body = "New tender is published. You can see information and fill the form for participate here - http://localhost:4200/tender";

                smpt.Port = 587;
                smpt.Credentials = new System.Net.NetworkCredential("hospitalssystem@gmail.com", "bolnica123");
                smpt.EnableSsl = true;
                smpt.Send(email);
            }
            catch (SmtpException ex)
            {
                Console.WriteLine("ERROR!");
            }
        }
    }
}