using AsianApi.Api.Helper;
using AsianApi.Model;
using AsianApi.Api.Events;
using AsianApi.Api.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Data.Entity.Migrations;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Collections.Specialized;

namespace AsianApi.Api
{
    class ApiAsian
    {
        public delegate void EventContainer(AccountApi account);

        public event EventContainer afterLogin;
      //  public event EventContainer afterLogout;
        protected AccountApi account;

        public ApiAsian(AccountApi accountApi)
        {
            account = accountApi;
            afterLogin += LoginHandler.Register;
        }

        //webapiuser31
        //SR8J@q=A
        /**
         * Login to api
         * 
         * @param String userName
         * @param String password
         * 
         * @return void
         */
        public void Login(String userName, String password)
        {
           
            var helper = new Data();
            IDictionary<string,string> data = new Dictionary<string,string>();
            IDictionary<string, string> head = new Dictionary<string, string>();
            Int32 timestamp = (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;

            data["username"] = userName;
            data["password"] = password;

            head["timestamp"] = timestamp.ToString();
            head["accept"] = helper.getOutputType();

            string token = helper.getToken(userName, password, timestamp.ToString());

            head["token"] = token;

            JToken responseJSON = sendRequest(data, head, helper.getLoginUrl());

            account.Id = (int)responseJSON.SelectToken("id");
            account.Username = userName;
            account.Token = (string)responseJSON.SelectToken("token");
            

            afterLogin(account);

            return;
        }

        public JToken GetFeeds()
        {
            var helper = new Data();

            IDictionary<string, string> data = new Dictionary<string, string>();
            IDictionary<string, string> head = new Dictionary<string, string>();

            data["username"] = account.Username;
            data["marketTypeId"] = account.MarketTypeId.ToString();
            data["sports"] = account.sportsType.ToString();
            head["token"] = account.Token;
            head["accept"] = helper.getOutputType();

            JToken responseJson = sendRequest(data, head, helper.getFeedsUrl());

            if (!ApiModel.Valid(responseJson))
            {
                throw new Exception("Feeds Error");
            }

            return responseJson;
        }

        public JToken GetLeagues()
        {
            var helper = new Data();
            IDictionary<string, string> data = new Dictionary<string, string>();
            IDictionary<string, string> head = new Dictionary<string, string>();

            data["username"] = account.Username;
            data["marketTypeId"] = account.MarketTypeId.ToString();
            data["sports"] = account.sportsType.ToString();

            head["token"] = account.Token;
            head["accept"] = helper.getOutputType();

            JToken responseJson = sendRequest(data, head, helper.getLeaguesUrl());

            if (!ApiModel.Valid(responseJson))
            {
                throw new Exception("Leagues Error");
            }

            return responseJson;
        }

        /**
        * Send request to API
        * 
        * @param IDictionary<string, string> data
        * @param IDictionary<string, string> head
        * @param string                      url
        *
        * @return JToken
        */

        public static JToken sendRequest(IDictionary<string, string> data, IDictionary<string, string> head, string url)
        {
            WebClient client = new WebClient();

            foreach (KeyValuePair<string, string> element in data)
            {
                client.QueryString.Add(element.Key, element.Value);
            }

            foreach (KeyValuePair<string, string> element in head)
            {
                client.Headers.Add(element.Key, element.Value);
            }
            try
            {
                return JObject.Parse(client.DownloadString(url));
            } 
            catch (WebException e)
            {
                throw new Exception(e.Status.ToString());
            }
           
        }
    }
}