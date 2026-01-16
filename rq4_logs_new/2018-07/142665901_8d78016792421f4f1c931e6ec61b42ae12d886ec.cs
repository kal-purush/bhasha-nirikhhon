using Windows.UI.Xaml.Controls;

namespace AirporClientUWP.Views
{
    public sealed partial class MainPageView : Page
    {
        public MainPageView()
        {
            this.InitializeComponent();
            contentFrame.Navigate(typeof(PilotsView));
        }

        private void SelectionChanged(NavigationView sender, NavigationViewSelectionChangedEventArgs args)
        {
            NavigationViewItem item = args.SelectedItem as NavigationViewItem;

            switch (item.Tag)
            {
                case "Pilots":
                    contentFrame.Navigate(typeof(PilotsView));
                    break;

                case "Stewardesses":
                    contentFrame.Navigate(typeof(StewardessView));
                    break;

                case "Crews":
                    contentFrame.Navigate(typeof(DepartureView));
                    break;

                case "Tickets":
                    contentFrame.Navigate(typeof(TicketView));
                    break;

                case "Flights":
                    contentFrame.Navigate(typeof(FlightsView));
                    break;

                case "Aeroplanes":
                    contentFrame.Navigate(typeof(AeroplaneView));
                    break;

                case "AeroplaneTypes":
                    contentFrame.Navigate(typeof(AeroplaneTypeView));
                    break;

                case "Departures":
                    contentFrame.Navigate(typeof(DepartureView));
                    break;
            }
        }
    }
}