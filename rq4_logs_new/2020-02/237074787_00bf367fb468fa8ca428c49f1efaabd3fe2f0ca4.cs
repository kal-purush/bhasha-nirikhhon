using MailSender.lib.Entities.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MailSender.lib.Entities
{
    /// <summary>
    /// Задача планировщика
    /// </summary>
    public class SchedulerTask:BaseEntity
    {
        /// <summary>
        /// Время выполнения задания
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// Отправитель письма
        /// </summary>
        public Sender Sender { get; set; }
        /// <summary>
        /// Получатели писем
        /// </summary>
        public MailingList Recipients { get; set; }
        /// <summary>
        /// Сервер отправителя
        /// </summary>
        public Server Server { get; set; }
        /// <summary>
        /// Содержание письма
        /// </summary>
        public Mail Mail { get; set; }
    }
}