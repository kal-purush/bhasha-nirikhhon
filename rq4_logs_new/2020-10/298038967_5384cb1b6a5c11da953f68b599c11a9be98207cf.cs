using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xamarin.Essentials;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace COP4331_RestaurantSystem_DavidGreen
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class MenuPage : ContentPage
    {

        int onAppearingCounter = 0;
        public MenuPage()
        {
            InitializeComponent();
        }

        protected async override void OnAppearing()
        {
            base.OnAppearing();

            // There is a bug in Xamarin forms where OnAppearing runs twice for no reason, so use this counter to only run 1/2 of the time
            onAppearingCounter++;
            if(onAppearingCounter % 2 == 1)
            {
                RestService service = new RestService();
                await service.Initialize();
                var menuItems = await service.GetMenuItems();
                menuListView.ItemsSource = menuItems;
            }
        }

        private async void menuListView_Refreshing(object sender, EventArgs e)
        {
            RestService service = new RestService();
            await service.Initialize();
            var menuItems = await service.GetMenuItems();
            menuListView.ItemsSource = menuItems;
            menuListView.IsRefreshing = false;
        }

        private void flushTokenButton_Clicked(object sender, EventArgs e)
        {
            SecureStorage.Remove("apiToken");
            SecureStorage.Remove("email");
        }
    }
}