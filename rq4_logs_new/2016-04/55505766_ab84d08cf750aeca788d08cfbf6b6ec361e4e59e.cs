using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;

namespace MVCTest.Controllers
{
    public class TestController : Controller
    {
        public async Task<ActionResult> WaitAsyncTest()
        {
            var client = new HttpClient();
            var t1 = client.GetStringAsync("http://localhost:36020/api/test/sleep/10000");
            var t2 = client.GetStringAsync("http://localhost:36020/api/test/sleep/10000");
            var cnt1 = await t1;
            await t2;
            return Content(cnt1);
        }

        public Task<ActionResult> TaskSyncTest()
        {
            var cnt1 = string.Empty;
            var cnt2 = string.Empty;
            return Task.Factory.StartNew(() =>
            {
                var client = new WebClient();
                cnt1 = client.DownloadString("http://localhost:36020/api/test/sleep/10000");
                cnt2 = client.DownloadString("http://localhost:36020/api/test/sleep/10000");
            }).ContinueWith<ActionResult>(task =>
            {
                return Content(cnt1);
            });
        }

        public ActionResult SyncTest()
        {
            var client = new WebClient();
            var cnt1 = client.DownloadString("http://localhost:36020/api/test/sleep/10000");
            var cnt2 = client.DownloadString("http://localhost:36020/api/test/sleep/10000");
            return Content(cnt1);
        }
    }
}