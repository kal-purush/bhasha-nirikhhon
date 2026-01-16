using System.Net.Mail;
using System.Net;
using System.ComponentModel.DataAnnotations;

namespace SGULibraryBE.Utilities
{
    public class MailUtil
    {
        const string EMAIL = "quangdeeptry1911@gmail.com";
        const string PASSWORD = "hmayehbweofjrkzu";
        const string NAME = "Quangdz";
        public static void SendEmail(string email, string subject, string body)
        {
            var fromAddress = new MailAddress(EMAIL, NAME);
            var toAddress = new MailAddress(email);
            const string fromPassword = PASSWORD;

            var smtp = new SmtpClient
            {
                Host = "smtp.gmail.com",
                Port = 587,
                EnableSsl = true,
                DeliveryMethod = SmtpDeliveryMethod.Network,
                UseDefaultCredentials = false,
                Credentials = new NetworkCredential(fromAddress.Address, fromPassword)
            };

            using (var message = new MailMessage(fromAddress, toAddress)
            {
                Subject = subject,
                Body = body
            })
            {
                smtp.Send(message);
            }
        }


        public static string generateCodeOTP()
        {
            Random random = new Random();
            string code = "";
            for (int i = 0; i < 6; i++)
            {
                code += random.Next(0, 10).ToString();
            }
            return code;
        }


        public static bool isExpired(DateTime time)
        {
            return time < DateTime.Now;
        }
    }
}