using SendGrid;
using SendGrid.Helpers.Mail;
using System.Net.Mail;


namespace SistemaDeNotificaciones
{
    public class EnviarCorreo
    {



        public async Task EnviarEmail()
        {
            var apiKey = Environment.GetEnvironmentVariable("SENDGRID_API_KEY");
            if (string.IsNullOrEmpty(apiKey))
            {
                throw new InvalidOperationException("SendGrid API key is not set in the environment variables.");
            }

            var client = new SendGridClient(apiKey);
            var from = new EmailAddress("cobros257982198596@gmail.com", "Sistema De Cobros");
            var subject = "Primer EMAIL";
            var to = new EmailAddress("info@zion.com.uy", "ZionWeb");
            var plainTextContent = "Primera prueba mails";
            var htmlContent = "";
            var msg = MailHelper.CreateSingleEmail(from, to, subject, plainTextContent, htmlContent);
            var response = await client.SendEmailAsync(msg);
        }


    }

}