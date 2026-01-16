using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace TripPlanner.Logic.Services.EmailService.Smtp
{
    public class RealSmtpClient : ISmtpClient
    {
        private readonly SmtpOptions config;
        public RealSmtpClient(IOptions<SmtpOptions> options )
        {
           config = options.Value;
        }

        public async Task SendMailAsync(MailMessage message)
        {
            SmtpClient client = new SmtpClient(config.Host, Convert.ToInt32(config.Port));
            client.EnableSsl = config.EnableSsl;
            client.Credentials = new NetworkCredential("dragos.boboluta@nagarro.com", "Aezakmi20021995@");
            
            await client.SendMailAsync(message);
           
        }
    }
}