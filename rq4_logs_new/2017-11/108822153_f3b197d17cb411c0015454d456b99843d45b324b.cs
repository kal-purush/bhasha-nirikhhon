using DCode.Common;
using DCode.Models.Email;
using DCode.Models.Reporting;
using DCode.Services.Reporting;
using Ninject;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DCode.ScheduledTasks.TaskNotifications
{
    internal class DailyNotificationsOperation : IReportingExecutor
    {
        private readonly IReportingService _reportingService;

        public DailyNotificationsOperation(IKernel kernel)
        {
            _reportingService = kernel.Get<ReportingService>();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public void Invoke()
        {
            var skills
                = _reportingService.GetSkillsForNewTasksAddeddYesterday();

            if (skills == null)
            {
                throw new Exception();
            }

            var notifications = GetNotificationsFromSkills(skills);

            EmailHelper.SendBulkEmail(notifications);
        }

        private IEnumerable<Notification> GetNotificationsFromSkills(IEnumerable<string> skills)
        {
            List<Notification> notifications = null;

            if (skills != null && skills.Any())
            {
                notifications = new List<Notification>();
            }

            notifications.AddRange(skills.Select(skill =>
            {
                var recipients
                  = _reportingService.GetSubscribedUserForTask(skill);

                return new Notification
                {
                    BccAddresses = recipients?.ToList(),
                    //TODO:(What is to be added)
                    Content = "Content for Notifications",
                    Subject = $"TechX :: New tasks for{skill}"
                };
            }));

            return notifications;
        }
    }
}