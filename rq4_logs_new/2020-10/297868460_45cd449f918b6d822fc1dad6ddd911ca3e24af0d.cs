using MailClient.lib.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Mail;
using System.Text;
using System.Threading;

namespace MailClient.lib.Service
{
    public class SmtpMailService : IMailService
    {
        public IMailSender GetSender(string ServerAddress, int ServerPort, bool UseSSL, string UserLogin, string UserPassword, string UserName = null, bool SendMsg = true)
        {
            return new SmtpMailSender(
                ServerAddress,
                ServerPort,
                UseSSL,
                UserLogin,
                UserPassword,
                UserName,
                SendMsg
                );
        }
    }
    public class SmtpMailSender : IMailSender
    {
        private readonly string _ServerAddress;

        private readonly int _ServerPort;

        private readonly bool _UseSSL;

        private readonly string _UserLogin;

        private readonly string _UserPassword;

        private readonly string _UserName;

        //Для имитации отправки
        private readonly bool _SendMsg;

        public SmtpMailSender(string ServerAddress, int ServerPort, bool UseSSL, string UserLogin, string UserPassword, string UserName = null, bool SendMsg=true)
        {
            _ServerAddress = ServerAddress;
            _ServerPort = ServerPort;
            _UseSSL = UseSSL;
            _UserLogin = UserLogin;
            _UserPassword = UserPassword;
            _UserName = UserName;
            _SendMsg = SendMsg;
        }

        public void Send(string RecipientAddress, string Subject, string Body)
        {
            MailAddress from;
            if (_UserName is null)
            {
                from = new MailAddress(_UserLogin);
            }
            else
            {
                from = new MailAddress(_UserLogin, _UserName);
            }
            var to = new MailAddress(RecipientAddress);
            using (var message = new MailMessage(from, to))
            {
                message.Subject = Subject;
                message.Body = Body;
                using (var client = new SmtpClient(_ServerAddress, _ServerPort))
                {
                    client.EnableSsl = _UseSSL;
                    client.UseDefaultCredentials = false;
                    client.Credentials = new NetworkCredential
                    {
                        UserName = _UserLogin,
                        Password = _UserPassword
                    };
                    try
                    {
                        if (_SendMsg)
                        {
                            client.Send(message);
                        }
                        else
                        {
                            //Имитация отправки
                            Thread.Sleep(2000);
                        }

                    }
                    catch (Exception e)
                    {
                        Trace.TraceError(e.ToString());
                        throw;
                    }
                }
            }
        }
    }
}