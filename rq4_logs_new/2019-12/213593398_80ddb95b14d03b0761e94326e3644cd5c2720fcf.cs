using MVCVitec.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace MVCVitec.ApiLogic
{
    public class CampaignConnection
    {
        private readonly string link = "https://localhost:44339/api/campaigns";

        public List<Campaign> GetData()
        {
            List<Campaign> list = new List<Campaign>();

            HttpWebRequest WebReq = (HttpWebRequest)WebRequest.Create(
                    string.Format(link));

            WebReq.Method = "GET";

            HttpWebResponse WebResp = (HttpWebResponse)WebReq.GetResponse();
            string jsonString;
            using (Stream stream = WebResp.GetResponseStream())
            {
                StreamReader reader = new StreamReader(stream, System.Text.Encoding.UTF8);
                jsonString = reader.ReadToEnd();
            }
            return list = JsonConvert.DeserializeObject<List<Campaign>>(jsonString);
        }


        public string DeleteData(int id)
        {
            string json;
            string deleteApiLink = link;
            deleteApiLink = deleteApiLink + "/" + id;

            HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(deleteApiLink);
            webRequest.Method = "DELETE";

            HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse();
            using (StreamReader reader = new StreamReader(response.GetResponseStream()))
            {
                json = reader.ReadToEnd();
            }

            return json;
        }

        public string PostData(Campaign campaign)
        {
            string payLoad = JsonConvert.SerializeObject(campaign);
            Uri uri = new Uri(link);
            HttpContent content = new StringContent(payLoad, Encoding.UTF8, "application/json");
            string response = PostCampaignData(uri, content).ToString();
            return response;
        }
        private async Task<string> PostCampaignData(Uri uri, HttpContent content)
        {
            string response = string.Empty;
            using (HttpClient client = new HttpClient())
            {
                HttpResponseMessage result = await client.PostAsync(uri, content);

                if (result.IsSuccessStatusCode)
                {
                    response = result.StatusCode.ToString();
                }
            }
            return response;
        }
    }
}