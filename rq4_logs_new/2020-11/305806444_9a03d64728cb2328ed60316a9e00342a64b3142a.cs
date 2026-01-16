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
    /// <summary>
    /// Interaction logic for AddDevicePage.xaml
    /// </summary>
    public partial class AddDevicePage : Page
    {
        public AddDevicePage()
        {
            InitializeComponent();
        }

        private void ButtonCancel(object sender, RoutedEventArgs e)
        {
            DevicesPage devicesPage = new DevicesPage();
            this.NavigationService.Navigate(devicesPage);
        }
    }
}