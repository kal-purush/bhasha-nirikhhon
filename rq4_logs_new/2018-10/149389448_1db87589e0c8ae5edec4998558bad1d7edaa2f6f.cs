using MAJServices.Models;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace MAJServices.Controllers
{
    [Route("api/push")]
    public class PushNotificationController
    {
        [Route("send")]
        public async Task<bool> SendPushNotification(PostWithoutUserDto post)
        {
            var applicationID = "AAAAC0su3P0:APA91bGc35v9Am1ME4h_lL6UyRV1SriTdv-jXu6lWgP_7Xc59XU1C2pXWk2FsKddVK8jt1lnG7E3WZcthtpi97Bp3RDbgW_IFsr8Y0KKaPhzsVusFW39K65DYFgpTiKHD1UakNJlgNEM";
            var senderId = "48506002685";
            var deviceId = "e-IzCLPMYhE:APA91bFYJCQercJzqcPduaw0x8Vta6IRTPDOOPa6Sxb3M6yZyulzGJOrp8l-7QCykq1NqIWDU0_V0lqHa04EbMOsI4V80iMekSDf7k6Ma9WY9GZc5ao3Qf5Q6T95ZK1h9dRAqEMW-U81";

            using (var client = new HttpClient())
            {
                //do something with http client
                client.BaseAddress = new Uri("https://fcm.googleapis.com");
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", $"key={applicationID}");
                client.DefaultRequestHeaders.TryAddWithoutValidation("Sender", $"id={senderId}");
                var data = new
                {
                    to = deviceId,
                    notification = new
                    {
                        body = post.Description,
                        title = post.Title,
                        icon = "myicon",
                        color = "#FFC300"
                    },
                    priority = "high"

                };
                var json = JsonConvert.SerializeObject(data);
                var httpContent = new StringContent(json, Encoding.UTF8, "application/json");
                var result = await client.PostAsync("/fcm/send", httpContent);
            }
            return true;
        }
    }
}