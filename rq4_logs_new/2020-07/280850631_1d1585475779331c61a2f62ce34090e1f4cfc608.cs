using Nexmo.Api;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Covid19TemperatureAPI.Mobile
{
    public static class SMSSender
    {
        public static bool SendSMS(string ApiKey, string ApiSecret, string from, string to, string msg)
        {
            try
            {
                var client = new Client(creds: new Nexmo.Api.Request.Credentials
                {
                    ApiKey = ApiKey,
                    ApiSecret = ApiSecret
                });

                var results = client.SMS.Send(request: new SMS.SMSRequest
                {
                    from = from,
                    to = to,
                    text = msg
                });
                return true;
            }
            catch(Exception e)
            {

            }
            return false;
        }
    }
}