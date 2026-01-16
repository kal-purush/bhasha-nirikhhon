using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using JoyOI.Monitor.Models;
using System.Threading;

namespace JoyOI.Monitor.Controllers.OnlineJudge
{
    [Route("/OnlineJudge/Groups")]
    public class GroupsController : ChartController
    {
        [HttpGet("Chart")]
        public async Task<IActionResult> Chart(int start, int end, int interval, int timezoneoffset, CancellationToken token)
        {
            if (start == 0 || end == 0 || interval == 0)
            {
                Response.StatusCode = 400;
                return Json(null);
            }
            var scaling = new ChartScaling(start, end, interval);
            return Json(await GetData(
                Judge,
                @"SELECT 
                  FLOOR(UNIX_TIMESTAMP(CreatedTime) / @interval) * @interval as t,  
                  Count(Id) as c  
                  FROM joyoi_oj.groups 
                  GROUP BY t 
                  HAVING t >= @start AND t <= @end 
                  ORDER BY t DESC 
                  LIMIT 0, @points",
                scaling,
                this.DefaultLineChartRowFn(scaling, timezoneoffset, "新团队创建"),
                token
            ));
        }
    }
}