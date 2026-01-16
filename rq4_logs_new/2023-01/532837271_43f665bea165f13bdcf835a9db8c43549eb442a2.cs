using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace TripPlanner.Logic.Services.EmailService.Smtp
{
    public class SmtpClient : ISmtpClient
    {
        private readonly SmtpOptions _config;
        public SmtpClient( )
        {
            
        }

        public void SendMailAsync(MailMessage message)
        {
            var result = this._config;
            var x = 5 % 5;
        }
    }
}