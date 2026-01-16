using System;
using System.Net.Http;
using System.Net.Http.Headers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TestFramework;
using Newtonsoft.Json.Linq;

namespace MSGraphTest
{
    [TestClass]
    public class MSGraphSearchTest
    {
        private string hostName = "http://msgraph-staging-localization.azurewebsites.net";
        private string path = "Search/localfiles?q=authentication&target=docs-local";
        private string[] languages = { "/en-us/", "/de-de/", "/ja-jp/", "/zh-cn/" };

        #region Initiaize/cleanup
        [ClassInitialize]
        public static void ClassInitialize(TestContext context)
        {
            GraphBrowser.Initialize();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            GraphBrowser.Close();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            GraphBrowser.Goto(GraphBrowser.BaseAddress);
        }

        #endregion

        #region Test cases
        [TestMethod]
        public void BVT_Graph_Search_TC01_VerifyLocalDocsSearchService()
        {
            foreach (string language in this.languages)
            {
                string url = this.hostName + language + path;
                JArray searchResult = this.GetRequestResult(url);
                Assert.IsTrue(searchResult.Count > 0, "Search result didnt return any result", url);

                // get the url from the search result and invoke, to make sure that search links are workng fine
                url = (string)searchResult[0]["Url"];
                Assert.IsTrue(CheckUrl(this.hostName + url), "The url in search result is not valid");
            }
        }
         #endregion

        private bool CheckUrl(string url)
        {
            bool success = false;
            using (var client = new HttpClient())
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                var response = client.SendAsync(request).Result;
                string content = response.Content.ReadAsStringAsync().Result;
                success = !content.Contains("NotFound.htm");
            }
            return success;
        }
        private Newtonsoft.Json.Linq.JArray GetRequestResult(string url)
        {
            JArray data = new JArray();
            using (var client = new HttpClient())
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var response = client.SendAsync(request).Result;
                Assert.IsTrue(response.IsSuccessStatusCode, "service didnt return success", url);
                var resMsg = response.Content.ReadAsStringAsync().Result;

                data =(JArray)Newtonsoft.Json.JsonConvert.DeserializeObject(resMsg);
            }
            return data;
        }
    }
}