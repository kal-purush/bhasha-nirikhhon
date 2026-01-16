using Quartz;
using Quartz.Impl;
using System;
using System.Collections.Generic;

namespace TimeTriggerService
{
    public class JobScheduler
    {
        private IScheduler scheduler;
        private IEnumerable<IJob> jobs;
        public JobScheduler(IScheduler schedulerParam, IEnumerable<IJob> jobsParam)
        {
            this.scheduler = schedulerParam;
            this.jobs = jobsParam;
        }

        public IScheduler Start()
        {
            foreach (var job in jobs)
            {
                var task = job.GetType();
                var jobInfo = Attribute.GetCustomAttribute(task, typeof(JobInfoAttribute)) as JobInfoAttribute;
                var jobName = jobInfo == null ? task.Name : jobInfo.Name;
                var jobDetail = new JobDetailImpl(jobName, null, task);
                var trigger =
                    TriggerBuilder.Create()
                        .WithIdentity($"{jobName}Trigger", null)
                        .WithCronSchedule(jobInfo.CronExpression)
                        .ForJob(jobDetail)
                        .Build();
                scheduler.ScheduleJob(jobDetail, trigger);
            }
            scheduler.Start();

            return scheduler;
        }
    }
}