using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MailSender.lib.Entities;

namespace MailSender.lib.Data
{
    public static class TestData
    {
        public static List<Server> Servers { get; } = new List<Server>
        {
            new Server{Name = "Яндекс", Address="smtp.yandex.ru", Port=587, Login="UserName", Password="Password"},
            new Server{Name = "Mail.ru", Address="smtp.mail.ru", Port=587, Login="UserName", Password="Password"},
            new Server{Name = "GMail", Address="smtp.gmail.com", Port=587, Login="UserName", Password="Password"}
        };

        public static List<Sender> Senders { get; } = new List<Sender>
        {
            new Sender{Name = "Иванов", Address="ivanov@server.ru"},
            new Sender{Name = "Петров", Address="petrov@server.ru"},
            new Sender{Name = "Сидоров", Address="sidorov@server.ru"}
        };
    }
}