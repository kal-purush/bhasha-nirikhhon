using System.Net;
using System.Net.Mail;
using Microsoft.Extensions.Configuration;
using System;
using System.Text;
using BancoDeAlimentos.Configuration;
using BancoDeAlimentos.Constants;
using BancoDeAlimentos.Entities;

namespace BancoDeAlimentos.Services.BusinessLogic
{
    public class EmailSender
    {
        private readonly IConfiguration _configuration;

        private SmtpConfiguration SmtpConfiguration => _configuration.GetSection(ConfigurationSections.SmtpConfiguration)
                                                            .Get<SmtpConfiguration>();

        public EmailSender(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public void SendAwaitingRequestEmail(Organization organization)
        {

            var client = CreateSmtpClient();

            MailMessage mailMessage = new MailMessage();
            mailMessage.To.Add(organization.ResponsableEmail);
            mailMessage.To.Add("reggie.durgan@ethereal.email");
            mailMessage.Subject = "Solicitud creada - Banco de alimentos";
            mailMessage.Body = "Estimado/a " + organization.ResponsableFirstName + " " + organization.ResponsableLastName + ". Tu solicitud fué creada con éxito, a la brevedad será evaluada y te comunicaremos nuestra decisíon. Muchas gracias";
            mailMessage.From = new MailAddress(SmtpConfiguration.Sender);
            client.SendAsync(mailMessage, "");
        }

        public void SendApprovedRequestEmail(Organization organization)
        {

            var client = CreateSmtpClient();

            MailMessage mailMessage = new MailMessage();
            mailMessage.To.Add(organization.ResponsableEmail);
            mailMessage.To.Add("reggie.durgan@ethereal.email");
            mailMessage.Subject = "Solicitud aprovada - Banco de alimentos";
            mailMessage.Body = "Estimado/a " + organization.ResponsableFirstName + " " + organization.ResponsableLastName + ". Tu solicitud fué aprobada, la organización ya forma parte de nuestra lista de organizaciones receptoras";
            mailMessage.From = new MailAddress(SmtpConfiguration.Sender);
            client.SendAsync(mailMessage, "");
        }

        public void SendRejectedRequestEmail(Organization organization)
        {

            var client = CreateSmtpClient();

            MailMessage mailMessage = new MailMessage();
            mailMessage.To.Add(organization.ResponsableEmail);
            mailMessage.To.Add("reggie.durgan@ethereal.email");
            mailMessage.Subject = "Solicitud rechazada - Banco de alimentos";
            mailMessage.Body = "Estimado/a " + organization.ResponsableFirstName + " " + organization.ResponsableLastName + ". Lamentamos informarle que su solicitud fué rechazada, por cualquier consulta, contáctenos mediante nuestra web";

            client.SendAsync(mailMessage, "");
        }

        private SmtpClient CreateSmtpClient()
        {
            SmtpClient client = new SmtpClient(SmtpConfiguration.ServerUrl);
            client.UseDefaultCredentials = false;

            client.Credentials = new NetworkCredential(SmtpConfiguration.User, SmtpConfiguration.Password);
            client.EnableSsl = SmtpConfiguration.Tls;
            client.Port = SmtpConfiguration.Port;

            return client;
        }
       


    }
}