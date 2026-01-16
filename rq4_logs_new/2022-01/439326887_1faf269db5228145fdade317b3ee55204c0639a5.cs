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
using System.Windows.Shapes;

namespace _1Semprojekt2022_Golf.View
{
    /// <summary>
    /// Interaction logic for ZipWindow.xaml
    /// </summary>
    public partial class ZipWindow : Window
    {
        public ZipWindow(string hej)
        {
            InitializeComponent();
            txtCode.Text = hej;
        }

        private void cmdRemove_Click(object sender, RoutedEventArgs e)
        {

        }

        private void cmdOk_Click(object sender, RoutedEventArgs e)
        {

        }

        private void cmdCancel_Click(object sender, RoutedEventArgs e)
        {

        }
    }
}