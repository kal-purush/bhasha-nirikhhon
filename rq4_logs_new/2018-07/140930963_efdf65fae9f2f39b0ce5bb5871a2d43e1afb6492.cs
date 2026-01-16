using NotifyMe.Models.DbModels;
using NotifyMe.ViewModels;
using Rg.Plugins.Popup.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Maps;
using Xamarin.Forms.Xaml;

namespace NotifyMe.Views.Popups
{
	[XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class MapPopup 
	{
        private Location _location;

		public MapPopup (Location location)
		{
			InitializeComponent ();

            _location = location;

            var position = new Position(location.Latitude, location.Longitude);
            Latitude.Text = location.Latitude.ToString();
            Longitude.Text = location.Longitude.ToString();

            MiniMap.MoveToRegion(MapSpan.FromCenterAndRadius(position, Distance.FromKilometers(0.2)));

            var pin = new Pin
            {
                Label = location.Name,
                Position = position
            };
            MiniMap.Pins.Add(pin);
        }

        private async Task DeleteLocation()
        {
            var responce = await Application.Current.MainPage.DisplayAlert("Delete Location", "Are you sure?", "Yes", "No");

            if (responce)
            {
                new LocationsPage().DeleteLocation(_location);
                await PopupNavigation.PopAsync();
            }
            else { return; }
        }


    }
}