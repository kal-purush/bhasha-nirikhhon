using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

// by CSC
using System.Net;
using System.Net.Http;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ApiService
{
    [TestClass]
    public class AsyncCallWebService : WebApiService
    {
        public AsyncCallWebService()
        {
            this.ServiceUri = "http://wsf.cdyne.com/WeatherWS/Weather.asmx/";
        }

        
        public async Task<bool> TestShouldReturnWeatherForecastForNewYorkWithProxyServerAsyncHelper()
        {
            var cookies = new CookieContainer();

            var httpClientHandler = new HttpClientHandler
            {
                CookieContainer = cookies,
                UseCookies = true,
                UseDefaultCredentials = true,
                Proxy = new WebProxy("http://192.168.115.254:8080", false, new string[] { }),
                UseProxy = true
            };

            var parameters = new List<ApiServiceParameter>();
            const string methodName = "GetCityForecastByZIP";

            parameters.Add(new ApiServiceParameter { ParamName = "ZIP", ParamValue = "10001" });

            var res = await CallWebApiAsync(methodName, httpClientHandler, parameters);

            if (res != null)
            {
                var apiResult = await res.Content.ReadAsStringAsync();



                bool containsNewYork = apiResult.Contains("New York");

               
                return containsNewYork;
            }

            return false;
        }

        [TestMethod]
        public void TestShouldReturnWeatherForecastForNewYorkWithProxyServerAsync()
        {
            var res = TestShouldReturnWeatherForecastForNewYorkWithProxyServerAsyncHelper().Result;

            Assert.IsNotNull(res);
            Assert.Equals(true, res);
        }
    }
}