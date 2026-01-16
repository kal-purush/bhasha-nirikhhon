using Xamarin.UITest;

using Query = System.Func<Xamarin.UITest.Queries.AppQuery, Xamarin.UITest.Queries.AppQuery>;

namespace BluetoothApplication.UITests
{
    public class UITestPage : BasePage
    {
        readonly Query temperatureEntryBox;

        public UITestPage(IApp app, Platform platform) : base(app, platform, "UITest")
        {
            temperatureEntryBox = x => x.Marked("temp1");
        }

        public void ClickTemperatureEntryBox()
        {
            app.Tap(temperatureEntryBox);

            app.Screenshot("Temperature Entry Box Tapped.");
        }
    }
}