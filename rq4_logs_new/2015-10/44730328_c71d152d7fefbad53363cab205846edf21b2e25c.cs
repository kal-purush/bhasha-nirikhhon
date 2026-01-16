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

namespace Fido2016
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private void cmdStart_Click(object sender, RoutedEventArgs e)
        {
            MessageBox.Show("Klick");
        }

        private void numud_ValueChanged(object sender, EventArgs e)
        {
            try
            {
                switch (numud.Child.Text)
                {
                    case "3":
                        Spieler3.Visibility = Visibility.Visible;
                        player3.Visibility = Visibility.Visible;
                        Spieler4.Visibility = Visibility.Hidden;
                        player4.Visibility = Visibility.Hidden;
                        break;
                    case "4":
                        Spieler3.Visibility = Visibility.Visible;
                        player3.Visibility = Visibility.Visible;
                        Spieler4.Visibility = Visibility.Visible;
                        player4.Visibility = Visibility.Visible;
                        break;
                    default:
                        Spieler3.Visibility = Visibility.Hidden;
                        player3.Visibility = Visibility.Hidden;
                        Spieler4.Visibility = Visibility.Hidden;
                        player4.Visibility = Visibility.Hidden;
                        break;

                }

            }
             catch(NullReferenceException n)
            {
                //MessageBox.Show(n.Message);
            }


        }
    }
}