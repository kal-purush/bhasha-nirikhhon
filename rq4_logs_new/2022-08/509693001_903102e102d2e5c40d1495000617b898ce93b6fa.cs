using Hangfire;
using Lection_2_BL.Jobs;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Lection_2_02_07.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class RecurringJobsController : ControllerBase
    {
        private IServiceProvider _serviceProvider;

        public RecurringJobsController(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        [HttpPost]
        public async Task UpdateJobState(bool isStart, string jobName)
        {
            if(jobName == nameof(CountMonitorJob))
            {
                if (isStart)
                {
                    var job = (CountMonitorJob)_serviceProvider.GetService(typeof(CountMonitorJob));
                    RecurringJob
                        .AddOrUpdate(nameof(CountMonitorJob),
                            () => job.StartJob(), Cron.Minutely);
                }
                else
                {
                    RecurringJob.RemoveIfExists(nameof(CountMonitorJob));
                }
            }
        }
    }
}