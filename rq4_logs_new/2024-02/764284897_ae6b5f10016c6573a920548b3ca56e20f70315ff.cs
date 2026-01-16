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

namespace KALENDARV2
{
    /// <summary>
    /// Логика взаимодействия для MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private void WatchB_Click(object sender, RoutedEventArgs e)
        {
            //Window WatchWindow = new WatchWindow();
            //WatchWindow.Show();
        }

        private void ChangeB_Click(object sender, RoutedEventArgs e)
        {
            //Window ChangeWindow = new ChangeWindow();
            //ChangeWindow.Show();
        }
    }
}