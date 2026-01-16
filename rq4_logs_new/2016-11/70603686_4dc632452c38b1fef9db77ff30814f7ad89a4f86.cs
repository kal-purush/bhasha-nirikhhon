using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Net.Http;

namespace Clivis.Models.Nibe
{


    public class NibeUnit : IClimateSource
    {
        public string clientId { get; set; }
        public string secret { get; set; }
        public string userName { get; set; }
        public string passWord { get; set; }

        private string systemId { get; set; }

        private string code { get; set; }
        
        public string redirect_uri { get; set; }
        private NibeAuth nibeAuth = new NibeAuth();
       
        public void init(AppKeyConfig config)
        {
            login();
           // setDeviceAndModuleID();
        }

       /* public string outDoorTemperature {
            get
            {            
                try
                {
                    HttpClient client = new HttpClient();
                    string url = "https://api.nibeuplink.com" + nibeAuth.access_token + "&device_id=" + deviceId + "&module_id=" + moduleId + "&type=Temperature&limit=1&date_end=last&scale=30min";

                    var resp = client.GetAsync(url).Result;
                    var response = resp.Content.ReadAsStringAsync().Result;

                    dynamic data = JsonConvert.DeserializeObject(response);

                    string temperature = data.body[0].value[0][0]; // temperature

                    return temperature;

                }
                catch (Exception e)
                {

                }            

            return null;
            }
        }

        public string inDoorTemperature
        {
            get
            {
               try
                    {
                        HttpClient client = new HttpClient();
                        string url = "http://api.netatmo.net/api/getmeasure?access_token=" + nibeAuth.access_token + "&device_id=" + deviceId + "&type=Temperature&limit=1&date_end=last&scale=30min";                     

                        var resp = client.GetAsync(url).Result;
                        string response = resp.Content.ReadAsStringAsync().Result;
                        dynamic data = JsonConvert.DeserializeObject(response);
                        
                        string temperature = data.body[0].value[0][0]; // temperature

                        return temperature;
                }
                catch (Exception e)
                {

                }

                return null;
            }
        }
*/
        private void login()
        {                                    
            //Login  
            var pairs = new List<KeyValuePair<string, string>>
            {
                    new KeyValuePair<string, string>("grant_type", "authorization_code" ),
                    new KeyValuePair<string, string>("client_id", clientId),
                    new KeyValuePair<string, string>( "client_secret", secret),
                    new KeyValuePair<string, string>("code", code),
                    new KeyValuePair<string, string>( "redirect_uri", redirect_uri),
                    new KeyValuePair<string, string>( "scope", "READSYSTEM")
            };

            HttpClient client = new HttpClient();
            var outcontent = new FormUrlEncodedContent(pairs);
            Console.WriteLine(pairs.ToString());
            var response = client.PostAsync("https://api.nibeuplink.com/oauth/token", outcontent).Result;

            string contentResult = response.Content.ReadAsStringAsync().Result;
Console.WriteLine(contentResult);
            nibeAuth = JsonConvert.DeserializeObject<NibeAuth>(contentResult);
          
        }

     /*   private void setDeviceAndModuleID()
        {
            string response = "";

            try
            {
                HttpClient client = new HttpClient();
                string url = "http://api.netatmo.net/api/devicelist?access_token=" + nibeAuth.access_token;
                var resp = client.GetAsync(url).Result;
                response = resp.Content.ReadAsStringAsync().Result;
            }
            catch (Exception e)
            {
            }


            dynamic data = JsonConvert.DeserializeObject(response);
            deviceId = data.body.devices[0]._id;
            moduleId = data.body.modules[0]._id;
        }*/
        public ClimateItem latestReading(AppKeyConfig AppConfigs)
        {
            login();
            //setDeviceAndModuleID();
            ClimateItem reading = new ClimateItem();
           // reading.IndoorValue = this.inDoorTemperature;
           // reading.OutdoorValue = this.outDoorTemperature;
            reading.TimeStamp = DateTime.Now;
            return reading;
        }
    }
}