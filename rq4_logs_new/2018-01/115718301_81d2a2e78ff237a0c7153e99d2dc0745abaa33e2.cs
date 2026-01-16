using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using JoyOI.Monitor.Models;
using System.Threading;

namespace JoyOI.Monitor.Controllers
{
    [Route("/Activity")]
    public class ActivityController : ChartController
    {

        Dictionary<string, string> titles = new Dictionary<string, string>()
        {
            { "joyoi_oj", "OJ 活跃" },
            { "joyoi_forum", "论坛活跃" },
            { "joyoi_blog", "博客活跃" }
        };

        [HttpGet("Page")]
        public IActionResult Page(string db)
        {
            string title = null;
            if (titles.TryGetValue(db, out title))
            {
                return View("Views/Activity.cshtml", new ActivityMeta
                {
                    Title = title,
                    Database = db
                });
            }
            else {
                Response.StatusCode = 404;
                return Json(new { err = "activity not found" });
            }
        }

        [HttpGet("List")]
        public async Task<IActionResult> List(string db, int start, int end, int timezoneoffset, CancellationToken token)
        {
            if (start == 0 || end == 0 || db.Equals(""))
            {
                Response.StatusCode = 400;
                return Json(null);
            }
            var scaling = new ChartScaling(start, end, 1);
            return Json(await GetData(
                db,
                @"SELECT UserName, ActiveTime FROM aspnetusers WHERE 
                    UNIX_TIMESTAMP(ActiveTime) >= @start AND
                    UNIX_TIMESTAMP(ActiveTime) <= @end
                  ORDER BY ActiveTime DESC",
                scaling,
                (rows) => rows,
                token
            ));
        }
    }
}