using Microsoft.Playwright;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TechTalk.SpecFlow;

namespace calculator.frontend.tests.steps
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
        public async Task WhenNumberIsCheckedForSR(int number)
        {
            _scenarioContext.Add("number", number);
        }

        [When("Square Root is calculated")]
        public async Task IGetSquareRoot()
        {
            IPage page = _scenarioContext.Get<IPage>("page");
            var base_url = _scenarioContext.Get<string>("urlBase");
            var number = _scenarioContext.Get<int>("number");
            await page.GotoAsync($"{base_url}/Attribute");
            await page.FillAsync("#number", number.ToString());
            await page.ClickAsync("#attribute");
            var resultTask = page.WaitForSelectorAsync("#squareRoot", new PageWaitForSelectorOptions { State = WaitForSelectorState.Attached });
            var squareRoot = await page.InnerTextAsync("#squareRoot");
            _scenarioContext.Add("getSquareRoot", SquareRoot);
        }

        [Then("the answer of the square root is (.*)")]
        public async Task ItShouldBeSquareRoot(string expectedSquareRoot)
        {
            var page = (IPage)_scenarioContext["page"];
            var resultText = await page.InnerTextAsync("#sqrt");
            var getSquareRoot = _scenarioContext["getSquareRoot"].ToString();
            Assert.Equal(expectedSquareRoot, getSquareRoot);

        }
    }
}
}