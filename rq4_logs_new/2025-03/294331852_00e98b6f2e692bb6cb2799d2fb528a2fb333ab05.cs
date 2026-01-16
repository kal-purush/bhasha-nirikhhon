using CMDB.Domain.Entities;

namespace CMDB.UI.Specflow.Abilities.Pages.Kensington
{
    public class KensingtonAssignDevicePage : MainPage
    {
        public void SelectDevice(Device device)
        {
            SelectValueInDropDownByXpath("//select[@id='Device']", $"{device.AssetTag}");
        }
    }
}