using SimpleHomeAuto.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

namespace SimpleHomeAuto
{
    public sealed partial class ActionPage : Page
    {
        Scenario scenario;

        public ActionPage()
        {
            this.InitializeComponent();
        }

        protected override void OnNavigatedTo(NavigationEventArgs e)
        {
            base.OnNavigatedTo(e);
            scenario = (e.Parameter as Scenario) ?? new Scenario();
        }

        private async void OnLoaded(object sender, RoutedEventArgs e)
        {
            Results.Text = "";
            Working.IsActive = true;

            try
            {
                var url = scenario.Url;
                if (!string.IsNullOrEmpty(url))
                {
                    var client = new HttpClient();
                    await client.GetStringAsync(url);
                    client.ToString();
                    Results.Text = "OK.";
                }
            }
            catch(Exception ex)
            {
                ex.ToString();
                Results.Text = "oups :(";
            }

            Working.IsActive = false;
        }

        private void OnTapped(object sender, TappedRoutedEventArgs e)
        {
            this.Frame.Navigate(typeof(MainPage));
        }
    }
}