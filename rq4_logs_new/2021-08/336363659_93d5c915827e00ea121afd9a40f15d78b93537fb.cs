using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Essentials;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace CovidApp.Views
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class SettingsPage : ContentPage
    {
        public SettingsPage()
        {
            InitializeComponent();
            BindingContext = this;
            Stats.Text = $"Stats: {rememberValue}";
        }

        public bool rememberValue
        {
            get => Preferences.Get("Stats", true);
            set
            {
                Preferences.Set("Stats", value);
                OnPropertyChanged(nameof(rememberValue));
               
                if (value)
                {
                    Stats.Text = "Set right panels to Provincial Stats";
                }
                else
                {
                    Stats.Text = "Set right panels to National Stats";
                }
            }

        }
        
        public async void ButtonClicked(object sender, EventArgs e)
        {
             List<PolyInfo> regions = await GeoJsonHandling.InitGeoJSON();
             Application.Current.MainPage = new NavigationPage(new TrackerPage(regions));
        }

    }
}