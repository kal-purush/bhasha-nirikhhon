using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Runtime.Versioning;
using System.Text;

namespace Course3.Presenters
{

    class SendMessagePresenter
    {
        private MailSender _ms;
        private IMailData _mailData;
        public ObservableCollection<MailService> services = new ObservableCollection<MailService>();

        public SendMessagePresenter(IMailData mailData)
        {
            _mailData = mailData;
            FillMailServices();
            
        }

        private void FillMailServices()
        {
            using (StreamReader sr = new StreamReader(@"Resources\MailServicesData.txt"))
            {
                while (!sr.EndOfStream)
                {
                    services.Add(new MailService(sr.ReadLine()));
                }
            }
        }

        public void SendMessage()
        {
            _ms = new MailSender(_mailData, services);
            _ms.SendMessage();
        }
    }
}