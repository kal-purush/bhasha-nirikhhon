using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace PDGT.Pages
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class Viewguide : ContentPage
    {
        public Viewguide()
        {
            InitializeComponent();
        }

        private async void ExitGuide(object sender, EventArgs e)
        {
            await Navigation.PopModalAsync();
        }
    }
}