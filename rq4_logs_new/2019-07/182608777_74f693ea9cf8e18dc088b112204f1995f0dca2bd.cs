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

namespace Quartz.AV
{
    /// <summary>
    /// Interaction logic for CheckUpdater.xaml
    /// </summary>
    public partial class CheckUpdater : Page
    {

        public CheckUpdater()
        {
            InitializeComponent();
        }

        private void RedirectToVersionChecker(object sender, RoutedEventArgs e)
        {
            UpdateWrapper.NavigationService.Navigate(new VersionChecker());
        }

    }
}