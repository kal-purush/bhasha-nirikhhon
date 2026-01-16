using System.Windows;
using HOApp.ViewModel;
using DataAccess.Entity.Entities;

namespace HOApp.Views
{
    /// <summary>
    /// Interaction logic for StoreProductTransView.xaml
    /// </summary>
    public partial class StoreProductTransView : Window
    {
        public StoreProductTransView(Product prod)
        {
            InitializeComponent();
            this.DataContext = new StoreProductTransViewModel(prod);
        }
    }
}