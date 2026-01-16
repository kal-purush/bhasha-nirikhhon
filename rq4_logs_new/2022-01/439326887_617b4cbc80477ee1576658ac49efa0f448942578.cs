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

namespace _1Semprojekt2022_Golf
{
    /// <summary>
    /// Interaction logic for Create_route.xaml
    /// </summary>
    public partial class Create_route : Window
    {
        public Create_route()
        {
            InitializeComponent();
            this.Closing += new System.ComponentModel.CancelEventHandler(Create_route_Closing);
        }
        void Create_route_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            MainWindow secondWindow = new MainWindow();
            secondWindow.Show();
        }
    }
}