using System.Text;
using MailKit.Net.Smtp;
using MailKit.Security;
using MimeKit;
using RazorEngineCore;
using TsdDelivery.Application.Commons;
using TsdDelivery.Application.Interface;
using TsdDelivery.Application.Models.Mail;
using TsdDelivery.Domain.Entities;

namespace TsdDelivery.Application.Services;

public class MailService : IMailService
{
    private readonly AppConfiguration _configuration;

    public MailService(AppConfiguration appConfiguration)
    {
        _configuration = appConfiguration;
    }
    public async Task<bool> SendAsync(MailData mailData, CancellationToken ct)
    {
        try
        {
            // Initialize a new instance MimeMessage 
            var mail = new MimeMessage();

            //sender
            mail.Sender = MailboxAddress.Parse(_configuration.MailConfig.From);

            //Receiver
            foreach (string mailAddress in mailData.To)
            {
                mail.To.Add(MailboxAddress.Parse(mailAddress));
            }

            //Add content to MineMessage
            var body = new BodyBuilder();
            mail.Subject = mailData.Subject;
            body.HtmlBody = mailData.Body;
            mail.Body = body.ToMessageBody();

            //Send Email
            using var smtp = new SmtpClient();
            if (_configuration.MailConfig.UseSSL) await smtp.ConnectAsync(_configuration.MailConfig.Host, _configuration.MailConfig.Port, SecureSocketOptions.SslOnConnect, ct);
            if (_configuration.MailConfig.UseStartTls) await smtp.ConnectAsync(_configuration.MailConfig.Host, _configuration.MailConfig.Port, SecureSocketOptions.StartTls, ct);

            await smtp.AuthenticateAsync(_configuration.MailConfig.UserName, _configuration.MailConfig.Password, ct);
            await smtp.SendAsync(mail, ct);
            await smtp.DisconnectAsync(true, ct);

            return true;

        }
        catch (Exception ex)
        {
            return false;
        }
    }

    public async Task<string> GetEmailTemplateForgotPassword(string nameTemplate,string clientHost,string code, User user)
    {
        string mailTemplate = LoadTemplate(nameTemplate);
        EmailTemplateModel emailTemplateModel = new EmailTemplateModel
        {
            FirstName = user.FullName,
            LastName = null,
            Email = user.Email,
            URL = $"{clientHost}/resetpassword?code={code}"
        };
        IRazorEngine razorEngine = new RazorEngine();
        IRazorEngineCompiledTemplate modifiledMailTemplate = razorEngine.Compile(mailTemplate);
        return modifiledMailTemplate.Run(emailTemplateModel);
    }
    
    private string LoadTemplate(string nameTemplate)
    {
        string templatePath = Path.Combine(Directory.GetCurrentDirectory(), "Resources", $"{nameTemplate}.cshtml");
        using FileStream fileStream = new FileStream(templatePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using StreamReader sr = new StreamReader(fileStream, Encoding.Default);

        string mailTemplate = sr.ReadToEnd();
        sr.Close();
        return mailTemplate;
    }
}