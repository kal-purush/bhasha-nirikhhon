using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace COP4331_RestaurantSystem_DavidGreen
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    [QueryProperty("ID", "id")]
    [QueryProperty("Name", "name")]
    [QueryProperty("Category", "category")]
    [QueryProperty("Price", "price")]
    public partial class AddToCartPage : ContentPage
    {

        public String ID { get; set; }

        public String Name { get; set; }

        public String Category { get; set; }

        public String Price { get; set; }

        private Models.MenuItem item;

        public AddToCartPage()
        {
            InitializeComponent();
        }

        protected override void OnAppearing()
        {
            base.OnAppearing();
            item = new Models.MenuItem(){
                ID = Int32.Parse(ID),
                Name = Name,
                Category = Int32.Parse(Category),
                Price = Decimal.Parse(Price)
            };

            itemNameLabel.Text = item.Name;
            itemPriceLabel.Text = item.Price.ToString("F");
        }

        private async void backButton_Clicked(object sender, EventArgs e)
        {
            await Shell.Current.GoToAsync("..");
        }

        private async void addButton_Clicked(object sender, EventArgs e)
        {
            // Add item to cart here

            await Shell.Current.GoToAsync("..");
        }
    }
}