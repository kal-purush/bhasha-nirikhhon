using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using System.Threading;
using JoyOI.Monitor.Models;
using System.Collections;

// For more information on enabling MVC for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace JoyOI.Monitor.Controllers.UserCenter
{
    
    [Route("/OnlineJudge/Contest")]
    public class ContestsController : ChartController
    {
        [HttpGet("Ongoing")]
        public async Task<IActionResult> Chart(int start, int end, int interval, int timezoneoffset, CancellationToken token)
        {
            if (start == 0 || end == 0 || interval == 0)
            {
                Response.StatusCode = 400;
                return Json(null);
            }
            var scaling = new ChartScaling(start, end, interval);
            return Json(await GetChartData(
                JUDGE,
                @"SELECT 
                  UNIX_TIMESTAMP(Begin) AS s,
                  UNIX_TIMESTAMP(ADDTIME(Begin, Duration)) AS e
                  FROM joyoi_oj.contests 
                  WHERE 
                  Begin >= DATE_ADD(FROM_UNIXTIME(@start), interval -30 day) 
                  AND
                  Begin <  FROM_UNIXTIME(@end)
                  AND
                  ADDTIME(Begin, Duration) > FROM_UNIXTIME(@start)",
                scaling,
                (rows) => {
                    string color = "#008b00";
                    string title = "正在进行的比赛";
                    var rows_tuple = rows
                        .Select(d => (Convert.ToInt64(d["s"]), Convert.ToInt64(d["e"]))).ToList();
                    var labels = FillMissingAndSort(new List<(long, double)>(), scaling).Select(x => x.Item1);
                    var values = labels.Select(l =>
                    {
                        var r_start = l;
                        var r_end = l + scaling.Interval;
                        var count = 0;
                        foreach ((long, long) row in rows_tuple) {
                            if (row.Item2 >= r_start && row.Item1 <= r_end) {
                                count++;
                            }
                        }
                        return count;
                    })
                    .Select(i => (double)i);
                    var datasets = new List<ChartDataSet>()
                    {
                        new ChartDataSet {
                            Label = title,
                            Data = values,
                            Fill = false,
                            BackgroundColor = color,
                            BorderColor = color
                        }
                    };
                    return new Chart {
                        Title = title,
                        Type = "line",
                        Data = new ChartData
                        {
                            Labels = labels.Select(t => ConvertTime(t, timezoneoffset)).ToList(),
                            Datasets = datasets
                        },
                        Options = new
                        {
                            Scales = TimeScaleOption()
                        }
                    };
                },
                token
            ));
        }
    }
}