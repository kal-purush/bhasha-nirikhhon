using System;
using TechTalk.SpecFlow;

namespace RestBDDAutomationPack.Steps
{
    [Binding]
    public class ValidatePayoutsSteps
    {
        [Given(@"the valid endpoind and user details")]
        public void GivenTheValidEndpoindAndUserDetails()
        {
            ScenarioContext.Current.Pending();
        }
        
        [Given(@"the endpoints with unauthorized access")]
        public void GivenTheEndpointsWithUnauthorizedAccess()
        {
            ScenarioContext.Current.Pending();
        }
        
        [When(@"the call to get Api")]
        public void WhenTheCallToGetApi()
        {
            ScenarioContext.Current.Pending();
        }
        
        [Then(@"the statustcode should be (.*)")]
        public void ThenTheStatustcodeShouldBe(int p0)
        {
            ScenarioContext.Current.Pending();
        }
        
        [Then(@"the payout details should be correct")]
        public void ThenThePayoutDetailsShouldBeCorrect()
        {
            ScenarioContext.Current.Pending();
        }
        
        [Then(@"the payout details should be correct on (.*) and (.*) and Test and EUR")]
        public void ThenThePayoutDetailsShouldBeCorrectOnAndAndTestAndEUR(int p0, string p1)
        {
            ScenarioContext.Current.Pending();
        }
        
        [Then(@"the (.*) should be returned")]
        public void ThenTheShouldBeReturned(string p0)
        {
            ScenarioContext.Current.Pending();
        }
    }
}