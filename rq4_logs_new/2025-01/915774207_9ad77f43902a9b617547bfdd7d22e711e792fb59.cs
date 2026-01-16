using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using TechTalk.SpecFlow;

namespace calculator.lib.test.steps
{
    [Binding]
    public class SquareRootSteps
    {
        private readonly ScenarioContext _scenarioContext;
        public SquareRootSteps(ScenarioContext scenarioContext)
        {
            _scenarioContext = scenarioContext;
        }


        [Given(@"a number (.*) to calculate the Square Root")]
        public void WhenNumberIsCheckedForSR(int number)
        {
            _scenarioContext.Add("number", number);
        }


        [When(@"Square Root is calculated")]
        public void IGetSquareRoot()
        {
            using (var client = new HttpClient())
            {
                var urlBase = _scenarioContext.Get<string>("urlBase");
                var url = $"{urlBase}api/Calculator/";
                var api_call = $"{url}number_attribute?number={number}";
                var response = client.GetAsync(api_call).Result;
                response.EnsureSuccessStatusCode();
                var responseBody = response.Content.ReadAsStringAsync().Result;
                var jsonDocument = JsonDocument.Parse(responseBody);
                var result = jsonDocument.RootElement.GetProperty("sqrt").GetDouble();

                _scenarioContext.Add("getSquareRoot", result);
            }
        }


        [Then(@"the answer of the square root is (.*)")]
        public void ItShouldBeSquareRoot(double expectedSquareRoot)
        {
            var getSquareRoot = _scenarioContext.Get<double>("getSquareRoot");
            Assert.Equal(expectedSquareRoot, getSquareRoot);
        }

    }
}