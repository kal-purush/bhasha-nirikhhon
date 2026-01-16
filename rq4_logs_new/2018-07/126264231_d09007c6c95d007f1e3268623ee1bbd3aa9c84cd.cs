using System.Net.Mail;

namespace CodeForGood.Config
{
    public static class EmailHelper
    {
        private const string MailDomain = "@codeforgoodwm.org";

        public static MailAddress ToMailAddress(this TeamEmail email)
        {
            switch (email)
            {
                case TeamEmail.NonProfit:
                    return new MailAddress($"nonprofit{MailDomain}", "Code for Good Non Profit Information");
                case TeamEmail.Volunteer:
                    return new MailAddress($"volunteer{MailDomain}", "Code for Good Volunteer Information");
                case TeamEmail.Info:
                    return new MailAddress($"info{MailDomain}", "Code for Good Information");
                case TeamEmail.NoReply:
                    return new MailAddress($"no-reply{MailDomain}", "No Reply");
                default:
                    return new MailAddress($"admin{MailDomain}", "Code for Good Admin");
            }
        }
    }
}