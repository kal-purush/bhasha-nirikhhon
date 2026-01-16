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

namespace Repair_Service
{
    public partial class AddEditBrandsPage : Page
    {
        public AddEditBrandsPage()
        {
            InitializeComponent();
        }

        #region BUTTONS
        private void OkButton_Click(object sender, RoutedEventArgs e)
        {
            LoadBrandsPage();
        }

        private void CancelButton_Click(object sender, RoutedEventArgs e)
        {
            LoadBrandsPage();
        }

        private void LoadBrandsPage()
        {
            BrandsPage brandsPage = new BrandsPage();
            this.NavigationService.Navigate(brandsPage);
        }
        #endregion
    }
}