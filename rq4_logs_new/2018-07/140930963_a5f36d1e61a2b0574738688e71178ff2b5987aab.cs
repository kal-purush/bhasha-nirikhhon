using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;
using Rg.Plugins.Popup.Services;

using NotifyMe.Models.DbModels;

namespace NotifyMe.Views.Popups
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class LocationAlertDetailsPopup
	{
        private Alert _alert;

        public LocationAlertDetailsPopup (Alert alert)
		{
			InitializeComponent ();

            _alert = alert;
            ReactivateBtn.IsVisible = alert.IsDisabled;

            AlertTitle.Text = alert.Title;
            Body.Text = alert.Description;
            Location.Text = alert.LocationName;
            CreatedOn.Text = alert.CreatedOn.ToString();
        }

        private async Task DisableAlert()
        {
            var responce = await Application.Current.MainPage.DisplayAlert("Disable Alert", "Are you sure?", "Yes", "No");

            if (responce)
            {
                new LocationAlertsPage().UpdateAlert(_alert, true);
                await PopupNavigation.Instance.PopAsync();
            }
            else { return; }
        }

        private async Task ReactivateAlert()
        {
            var responce = await Application.Current.MainPage.DisplayAlert("Reactivate Alert", "Are you sure?", "Yes", "No");

            if (responce)
            {
                new LocationAlertsPage().UpdateAlert(_alert, false);
                await PopupNavigation.Instance.PopAsync();
            }
            else { return; }
        }

        private async Task DeleteAlert()
        {
            var responce = await Application.Current.MainPage.DisplayAlert("Delete Alert", "Are you sure?", "Yes", "No");

            if (responce)
            {
                new LocationAlertsPage().DeleteAlert(_alert);
                await PopupNavigation.Instance.PopAsync();
            }
            else { return; }
        }
    }
}