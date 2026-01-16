using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SpamTool_Akhmerov.lib.Database;

namespace SpamTool_Akhmerov.lib.Data
{
    public class Scheduler
    {
        private readonly ObservableCollection<SchedulerTask> _Tasks;

        public ObservableCollection<SchedulerTask> Tasks => _Tasks;

        public Scheduler()
        {
            _Tasks = new ObservableCollection<SchedulerTask>
            {
                new SchedulerTask
                {
                    Mail = new Mail { Body = "Test mail 1", Subject = "Mail 1" },
                    Sender = new Sender { Email = "sender1@server.ru", Name = "SenderName1", Password = "SenderPassword" },
                    Recipients = new[]
                    {
                        new EmailRecipient { Id = 0, Name = "Recipient1", EmailAddress = "recipient1@mail.ru"},
                        new EmailRecipient { Id = 1, Name = "Recipient2", EmailAddress = "recipient2@mail.ru"},
                        new EmailRecipient { Id = 2, Name = "Recipient3", EmailAddress = "recipient3@mail.ru"}
                    },
                    Server = new MailServer { Address = "http://Server.ru", Port = 123, UseSSL = false },
                    Time = DateTime.Now.Add(TimeSpan.FromMinutes(20))
                },
                new SchedulerTask
                {
                    Mail = new Mail { Body = "Test mail 2", Subject = "Mail 2" },
                    Sender = new Sender { Email = "sender2@server.ru", Name = "SenderName2", Password = "SenderPassword" },
                    Recipients = new[]
                    {
                        new EmailRecipient { Id = 0, Name = "Recipient1", EmailAddress = "recipient1@mail.ru"},
                        new EmailRecipient { Id = 1, Name = "Recipient2", EmailAddress = "recipient2@mail.ru"},
                        new EmailRecipient { Id = 2, Name = "Recipient3", EmailAddress = "recipient3@mail.ru"}
                    },
                    Server = new MailServer { Address = "http://Server2.ru", Port = 321, UseSSL = false },
                    Time = DateTime.Now.Add(TimeSpan.FromMinutes(40))
                }
            };
        }

        public Scheduler(IEnumerable<SchedulerTask> tasks) => _Tasks = new ObservableCollection<SchedulerTask>(tasks);

        public void Start() { }

        public void AddTask(SchedulerTask task)
        {
            if (_Tasks.Contains(task)) return;
            _Tasks.Add(task);
        }

        public bool RemoveTask(SchedulerTask task) => _Tasks.Remove(task);
    }
}