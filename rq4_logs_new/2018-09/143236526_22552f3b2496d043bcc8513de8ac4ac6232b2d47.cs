using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Mvc;
using System.Web;
using CentralLogger.Model;
using System.Globalization;
using Microsoft.AspNetCore.SignalR;
using CentralLogger.Hubs;
using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Cryptography.KeyDerivation;
using CentralLogger.Services;
using System.Net.Mail;
using System.Net;
using Microsoft.Extensions.Configuration;

namespace CentralLogger.Controllers {
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class EmailController : ControllerBase {
        private readonly CentralLoggerContext db;
        private readonly IConfiguration configuration;
        public EmailController(CentralLoggerContext db, IConfiguration configuration) {
            this.configuration = configuration;
            this.db = db;
        }
        public async Task chkEmail(LogInfo datas) {
            var data = datas;
            if (data.LogLevel == LogLevel.Critical) {
                // var Email = db.Emails.Where(z => z.Enable == true && z.Application == data.Application ).Select(m => new { m.Email_1,  m.Email_2, m.Email_3 }).ToList();
                var Email1 = db.Emails.Where(z => z.Enable == true && z.Application == data.Application).Select(m => m.Email_1).ToList();
                var Email2 = db.Emails.Where(z => z.Enable == true && z.Application == data.Application).Select(m => m.Email_2).ToList();
                var Email3 = db.Emails.Where(z => z.Enable == true && z.Application == data.Application).Select(m => m.Email_3).ToList();
                var allEmail = Email1.Union(Email2).Union(Email3).ToArray();
                foreach (var email in allEmail) {
                    if (!string.IsNullOrEmpty(email)) {
                        await sendMail(data, email);
                    }
                }
            }
        }
        public async Task sendMail(LogInfo data, string Email) {
            string subject = $"Critical Alert {data.Application} [ {data.Ip} ]";
            string body = $"Found Critical:@Application : {data.Application}@Datetime : {data.DateTime}@Category : {data.Category}@IP : {data.Ip}@Message : {data.Message}";
            body = body.Replace("@", Environment.NewLine);
            string FromMail = configuration["Email:Account"];
            string Password = configuration["Email:Password"];
            string emailTo = Email;
            MailMessage mail = new MailMessage();
            SmtpClient SmtpServer = new SmtpClient("smtp.gmail.com");
            SmtpServer.UseDefaultCredentials = false;
            SmtpServer.EnableSsl = true;
            SmtpServer.Port = 587;
            SmtpServer.Credentials = new System.Net.NetworkCredential(FromMail, Password);
            mail.From = new MailAddress(FromMail);
            mail.To.Add(emailTo);
            mail.Subject = subject;
            mail.Body = body;
            await SmtpServer.SendMailAsync(mail);
        }

    }
}