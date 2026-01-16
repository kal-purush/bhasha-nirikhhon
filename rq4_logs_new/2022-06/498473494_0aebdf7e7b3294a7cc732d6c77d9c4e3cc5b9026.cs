using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace MacTesting
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class Favourites : ContentPage
    {
        public Favourites()
        {
            InitializeComponent();

            this.BindingContext = new[] { "Game 1", "Game 2", "Game 3", "Game 4" };
        }

        async void onItemTapped(object sender, ItemTappedEventArgs e)
        {
            if (e == null) return;
            await Navigation.PushAsync(new GameInfo());
            ((ListView)sender).SelectedItem = null;
        }
    }
}