using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Text;
using System.Threading.Tasks;

namespace DesignPatterns.BehavioralPatterns.TemplateMethod.Emails
{
    //superclasse ou classe base.
    public abstract class EmailBase
    {
        protected abstract void ValidateRecipient(string recipient);
        protected abstract void ValidateMessage(string message);
        protected abstract void Append();

        public void ValidateEmail(string emailaddress)
        {
            try
            {
                MailAddress m = new MailAddress(emailaddress);
            }
            catch (FormatException)
            {
                throw new EmailException("Invalid e-mail address.");
            }
        }

        public void SendEmail(string recipient, string message)
        {
            ValidateRecipient(recipient);
            ValidateMessage(message);
            Console.WriteLine(string.Format("@{0}, {1}", recipient, message));
            Append();
        }
    }
}