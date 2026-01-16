using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;

namespace TeamNotify
{
    public class connectInfo
    {
        public string title;
        public string description;
        public string imageUrl = "http://url_to_text";
    }

    public class JandiPayload
    {
        public string body;
        public string connectColor;
        public List<connectInfo> connectInfo;
    }

    public class JANDI : Notify
    {
        public JANDI(Dictionary<int, string> dicUrl)
        {
            _dicUrl = dicUrl;
        }

        /// <summary>
        /// 送出通知訊息
        /// </summary>
        /// <param name="message">訊息的內容字串</param>
        public void Notify(string message, int channel, string fromTo)
        {
            try
            {
                if (_dicUrl.ContainsKey(channel))
                {
                    _Url = _dicUrl[channel];

                    PostMessage(message, fromTo);
                }
                else
                {
                    Console.WriteLine($"channel={channel} not find");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        /// <summary>
        /// Post a message using simple strings
        /// </summary>
        /// <param name="text"></param>
        /// <param name="title"></param>
        protected void PostMessage(string text, string title)
        {
            List<connectInfo> info_ = new List<connectInfo>()
            {
                new connectInfo()
                {
                    title = title,
                    description = text,
                    imageUrl = ""
                }
            };

            JandiPayload payload = new JandiPayload()
            {
                body = "你有一個通知",
                connectColor = "#FAC11B",
                connectInfo = info_
            };

            PostMessage(payload);
        }

        /// <summary>
        /// Post a message using a Payload object
        /// </summary>
        /// <param name="payload"></param>
        protected void PostMessage(JandiPayload payload)
        {
            //建立 HttpClient
            HttpClient client = new HttpClient() { BaseAddress = new Uri(_Url) };

            // 將 data 轉為 json
            string json = JsonConvert.SerializeObject(payload);
            // 將轉為 string 的 json 依編碼並指定 content type 存為 httpcontent
            HttpContent contentPost = new StringContent(json, Encoding.UTF8, "application/json");

            // 發出 post 並取得結果
            HttpResponseMessage response = client.PostAsync(new Uri(_Url), contentPost).GetAwaiter().GetResult();
            // 將回應結果內容取出並轉為 string 再透過 linqpad 輸出
            response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        }
    }
}