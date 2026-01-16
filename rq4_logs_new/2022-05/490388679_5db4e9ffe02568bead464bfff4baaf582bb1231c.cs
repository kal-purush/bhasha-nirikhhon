using System.Net;
using System.Net.Mail;

namespace Blog.Services;

public class EmailService
{
    public bool Send(
        string toName,
        string toEmail,
        string subject,
        string body,
        string fromName = "<your_name>",
        string fromEmail = "<your_email>")
    {
        var smtpClient = new SmtpClient(Configuration.Smtp.Host, Configuration.Smtp.Port);

        smtpClient.Credentials = new NetworkCredential(
            userName: Configuration.Smtp.Username,
            password: Configuration.Smtp.Password);
        smtpClient.DeliveryMethod = SmtpDeliveryMethod.Network;
        smtpClient.EnableSsl = true;

        var mail = new MailMessage();

        mail.From = new MailAddress(address: fromEmail, displayName: fromName);
        mail.To.Add(new MailAddress(address: toEmail, displayName: toName));
        mail.Subject = subject;
        mail.Body = body;
        mail.IsBodyHtml = true;

        try
        {
            smtpClient.Send(mail);
            return true;
        }
        catch (System.Exception ex)
        {
            return false;
        }
    }
}