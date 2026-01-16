using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Navigation;

// The Blank Page item template is documented at http://go.microsoft.com/fwlink/?LinkId=234238

namespace QuanLyThuChi.GUI
{
    /// <summary>
    /// An empty page that can be used on its own or navigated to within a Frame.
    /// </summary>
    public sealed partial class QuanLyTK : Page
    {
        public QuanLyTK()
        {
            this.InitializeComponent();
        }

        private void HamburgerButton_Click(object sender, RoutedEventArgs e)
        {
            MySplitView.IsPaneOpen = !MySplitView.IsPaneOpen;
        }

        private void tapThemTK_Tapped(object sender, TappedRoutedEventArgs e)
        {
            Frame quanlyFrame = Window.Current.Content as Frame;
            quanlyFrame.Navigate(typeof(ThemTK));
        }

        private void btnXoa_Click(object sender, RoutedEventArgs e)
        {

        }

        private void lstbUser_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {

        }

        private void btnSua_Click(object sender, RoutedEventArgs e)
        {
            Frame quanlyFrame = Window.Current.Content as Frame;
            quanlyFrame.Navigate(typeof(SuaTK));
        }

        private void tapVi_Tapped(object sender, TappedRoutedEventArgs e)
        {
            Frame quanlyFrame1 = Window.Current.Content as Frame;
            quanlyFrame1.Navigate(typeof(QuanLyTK));
        }
    }
}