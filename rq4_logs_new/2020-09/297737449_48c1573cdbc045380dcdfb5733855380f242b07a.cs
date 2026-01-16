using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Text;

namespace Course3
{
    class MailSender
    {
        IMailData _mailData;
        private MailMessage _mm;

        public MailSender(IMailData mailData, ObservableCollection<MailService> services)
        {
            _mailData = mailData;
            var selectedService = services.Where(x => x.Name == _mailData.SeviceName).FirstOrDefault();

            var from = new MailAddress($"{_mailData.ServiceLogin}{selectedService.ServiceDomain}");
            var to = new MailAddress($"{_mailData.MailReciever}");
            _mm = new MailMessage(from, to);
            _mm.Subject = _mailData.MailTitle;
            _mm.Body = _mailData.MailText;
            _mm.IsBodyHtml = false;

            SmtpClient sc = new SmtpClient(selectedService.Url, selectedService.Port);
            sc.EnableSsl = true;
            sc.DeliveryMethod = SmtpDeliveryMethod.Network;
            sc.UseDefaultCredentials = false;
            sc.Credentials = new NetworkCredential($"{_mailData.ServiceLogin}{selectedService.ServiceDomain}", _mailData.ServicePassword);            

        }
    }
}