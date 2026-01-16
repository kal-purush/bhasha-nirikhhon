using TechTalk.SpecFlow;
using MobileUISpecTestFramework;

namespace MobileDeviceUISpecTests
{
    [Binding]
    public class BeforeAfterSteps
    {
        [BeforeScenario]
        public void BeforeSteps()
        {
            DeviceInitialiser.StartAppium();
            DeviceInitialiser.LaunchApplication();
        }

        [AfterScenario]
        public void AfterSteps()
        {
            DeviceInitialiser.Quit();
            DeviceInitialiser.KillAppium();
        }        
    }
}