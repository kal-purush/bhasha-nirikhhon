using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace GoShopping.Views
{
    /// <summary>
    /// Interaction logic for DishesListView.xaml
    /// </summary>
    public partial class DishesListView : UserControl
    {
        GoShoppingDbContext context = new GoShoppingDbContext();
        public DishesListView()
        {
            InitializeComponent();
            DataContext = context.Dishes.Select(x => x.Name).ToList();

        }

        private void OnUncheckItem(object sender, RoutedEventArgs e)
        {
            throw new NotImplementedException();
        }

        private void checkedListView_Checked(object sender, RoutedEventArgs e)
        {
            throw new NotImplementedException();
        }
    }
}