using Renci.SshNet;
using System;
using System.IO;
using System.Net.Mail;

namespace Backend.Service.SftpService
{
    public class SftpService : ISftpService
    {
        public SftpService()
        {

        }

        public void UploadFile(string file)
        {
           using (SftpClient client = new SftpClient(new PasswordConnectionInfo("192.168.0.13", "user", "password")))
           {
                client.Connect();
                client.UploadFile(File.OpenRead(file), @"" + Path.GetFileName(file), x => { Console.WriteLine(x); });
                SendNotification();
                client.Disconnect();
           }
        }

        public void SendNotification()
        {
            try
            {
                Console.WriteLine("E-mail se salje!");
                MailMessage email = new MailMessage();
                SmtpClient smpt = new SmtpClient("smtp.gmail.com");

                email.From = new MailAddress("hospitalssystem@gmail.com");
                email.To.Add("pharmacysistem@gmail.com");
                email.Subject = ("Nofication about send file");
                email.Body = "We sent you new file!";

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