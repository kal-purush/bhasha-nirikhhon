using System.Reflection.Metadata.Ecma335;
using MediatR;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using sib_api_v3_sdk.Api;
using sib_api_v3_sdk.Model;
using WolfDen.Application.Requests.DTOs.Attendence;
using WolfDen.Application.Requests.Queries.Attendence.DailyStatus;
using WolfDen.Domain.Entity;
using WolfDen.Infrastructure.Data;

namespace WolfDen.Application.Requests.Commands.Attendence.Email
{
    public class SendEmailCommandHandler: IRequestHandler<SendEmailCommand, string>
    {
        private readonly WolfDenContext _context;
        private readonly string _senderEmail;
        private readonly string _senderName;
        public SendEmailCommandHandler(WolfDenContext context, IConfiguration configuration)
        {
            _context = context;
            _senderEmail = configuration["BrevoApi:SenderEmail"];
            _senderName = configuration["BrevoApi:SenderName"];
        }
        public static void SendMail(string senderEmail, string senderName,
            string[] recieverEmail,
            string message,
            string subject,
            string[] ccEmails = null)
        {
            TransactionalEmailsApi apiInstance = new TransactionalEmailsApi();

            SendSmtpEmailSender sender = new SendSmtpEmailSender
            {
                Email = senderEmail,
                Name = senderName
            };

            List<SendSmtpEmailTo> to = new List<SendSmtpEmailTo>();
            if (recieverEmail != null)
                for (int i = 0; i < recieverEmail.Length; i++)
                {
                    to.Add(new SendSmtpEmailTo(recieverEmail[i]));
                }
            List<SendSmtpEmailCc> cc = new List<SendSmtpEmailCc>();
            if (ccEmails != null)
                for (int i = 0; i < ccEmails.Length; i++)
                {
                    cc.Add(new SendSmtpEmailCc(ccEmails[i]));
                }
            string htmlContent = null;
            string textContent = message;
            try
            {
                SendSmtpEmail sendSmtpEmail = new SendSmtpEmail(sender, to, null, cc, htmlContent, textContent, subject);
                CreateSmtpEmail result = apiInstance.SendTransacEmail(sendSmtpEmail);
                Console.WriteLine("Response" + result.ToJson());
            }
            catch (Exception e)
            {
                Console.WriteLine("Error" + e.Message);
            }
        }
        public async Task<string> Handle(SendEmailCommand request, CancellationToken cancellationToken)
        {
            string[] recieverEmail = new string[1];

            var recEmail = await _context.Employees.Where(x => x.Id == request.EmployeeId)
           .Select(x => x.Email).FirstOrDefaultAsync(cancellationToken);

            var recName = await _context.Employees.Where(x => x.Id == request.EmployeeId)
                .Select(x => x.FirstName + " " + x.LastName).FirstOrDefaultAsync(cancellationToken);

            if (recEmail != null)
            {
                recieverEmail[0] = recEmail;
            }

            var managerId = await _context.Employees.Where(x => x.Id == request.EmployeeId)
                .Select(x => x.ManagerId).FirstOrDefaultAsync();

            List<string> cCManagerEmail = new List<string>();

            List<string> ManagerEmail = new List<string>();
            if (managerId != null)
            {
                ManagerEmail = await FindManager(managerId, cCManagerEmail, cancellationToken);
            }
            string subject = "Attendence Low Warning";
            string message = $"{recName}'s working hour is less than required";
            SendMail(_senderEmail, _senderName, recieverEmail, message, subject, ManagerEmail.ToArray());
            return "success";
        }
        private async Task<List<string>> FindManager(int? managerId, List<string> cCManagerEmail, CancellationToken cancellationToken)
        {
            var manager = await _context.Employees.Where(x => x.Id == managerId)
                .Select(x => new
                {
                    x.Email,
                    x.ManagerId
                }).FirstOrDefaultAsync(cancellationToken);

            if (manager.ManagerId is null)
            {
                return cCManagerEmail;
            }
            else
            {
                var managerEmails = await FindManager(manager.ManagerId, cCManagerEmail, cancellationToken);
                cCManagerEmail.AddRange(managerEmails);
            }
            return cCManagerEmail;
        }
    }

}
