using BL.MailTemplates;
using DAL.Models;
using DAL.Repositories;
using Microsoft.Extensions.Configuration;
using SendGrid;
using SendGrid.Helpers.Mail;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BL.Mail
{
    public static class Mailer
    {
        private const string apiKey = "API_KEY";
        private static readonly SendGridClient sendGridClient = new SendGridClient(apiKey);

        public static async Task InviteTeamscan(TeamMember teamMember, Team team, User teamleader, Guid individualScoreId)
        {
            var sendGridMessage = new SendGridMessage();
            sendGridMessage.SetFrom("yanu.szapinszky@euri.com", "Euricom");
            sendGridMessage.AddTo(teamMember.Email, $"{teamMember.Firstname} {teamMember.Lastname}");

            var mailtemplate = new MailTemplateInviteTeamscan
            {
                Name = $"{teamMember.Firstname} {teamMember.Lastname}",
                TeamleaderName = $"{teamleader.Firstname} {teamleader.Lastname}",
                TeamName = team.Name,
                Url = $"http://localhost:3000/teamscan/{individualScoreId}"
            };

            sendGridMessage.SetTemplateId(mailtemplate.TemplateId);
            sendGridMessage.SetTemplateData(mailtemplate);

            var response = await sendGridClient.SendEmailAsync(sendGridMessage);
        }
    }
}