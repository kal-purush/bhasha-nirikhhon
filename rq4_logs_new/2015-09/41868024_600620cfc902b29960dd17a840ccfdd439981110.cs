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
using System.Windows.Navigation;
using System.Diagnostics;

namespace FZChat.Client.View
{
    /// <summary>
    /// FindPassword.xaml 的交互逻辑
    /// </summary>
    public partial class FindPassword : Window
    {
        public FindPassword()
        {
            InitializeComponent();
        }

        private void closebutton_MouseEnter(object sender, MouseEventArgs e)
        {
            closebutton.Background = new SolidColorBrush(Color.FromArgb(255, 255, 0, 0));
        }

        private void closebutton_MouseLeave(object sender, MouseEventArgs e)
        {
            closebutton.Background = new SolidColorBrush(Color.FromArgb(0, 255, 0, 0));
        }

        private void closebutton_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            closebutton.Background = new SolidColorBrush(Color.FromArgb(120, 255, 0, 0));
        }

        private void closebutton_MouseLeftButtonUp(object sender, MouseButtonEventArgs e)
        {
            this.Close();
        }

        private void minbutton_MouseEnter(object sender, MouseEventArgs e)
        {
            minbutton.Background = new SolidColorBrush(Color.FromArgb(120, 120, 80, 80));
        }

        private void minbutton_MouseLeave(object sender, MouseEventArgs e)
        {
            minbutton.Background = new SolidColorBrush(Color.FromArgb(0, 255, 0, 0));
        }

        private void minbutton_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            minbutton.Background = new SolidColorBrush(Color.FromArgb(255, 120, 80, 80));
        }

        private void minbutton_MouseLeftButtonUp(object sender, MouseButtonEventArgs e)
        {
            this.WindowState = WindowState.Minimized;
        }

        private void okborder_MouseEnter(object sender, MouseEventArgs e)
        {
            okborder.BorderThickness = new Thickness(2);
        }

        private void Border_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            this.DragMove();
        }


    }
}