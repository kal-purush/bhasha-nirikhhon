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

namespace MVCVitec.Data
{
    public class ProductConnection
    {
        private readonly string link = "https://localhost:44339/api/products";
        
        public List<Product> GetData()
        {
            List<Product> list = new List<Product>();

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
            return list = JsonConvert.DeserializeObject<List<Product>>(jsonString);
        }


        public string DeleteData(int id)
        {
            string json;
            string deleteApiLink = "link";
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

        public string PostData(Product product)
        {
            string payLoad = JsonConvert.SerializeObject(product);
            Uri uri = new Uri(link);
            HttpContent content = new StringContent(payLoad, Encoding.UTF8, "application/json");
            string response = PostProductData(uri, content).ToString();
            return response;
        }
        private async Task<string> PostProductData(Uri uri, HttpContent content)
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