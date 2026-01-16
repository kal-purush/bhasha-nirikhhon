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

namespace WPFDemo
{
    /// <summary>
    /// Interaction logic for AniWin.xaml
    /// </summary>
    public partial class AniWin : Window
    {
        /// <summary>
        /// loads the XAML code to show a square that is animated by reducing and then increasing its opacity.
        /// </summary>
        public AniWin()
        {
            InitializeComponent();
        }

        /// <summary>
        /// stops the animation and closes the window retruning to the menu.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void Button_Click(object sender, RoutedEventArgs e)
        {
            MainWindow mainWindow = new MainWindow();
            mainWindow.Show();
            this.Close();
        }
    }
}