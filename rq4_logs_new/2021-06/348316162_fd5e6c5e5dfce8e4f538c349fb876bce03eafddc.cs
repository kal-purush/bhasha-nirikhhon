using System;
using System.Collections.Generic;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace EventPlanner.Pages
{
    /// <summary>
    /// Interaction logic for MessagesPage.xaml
    /// </summary>
    public partial class MessagesPage : Page
    {
        public MessagesPage()
        {
            InitializeComponent();
        }

        private void messageTextBox_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Enter)
            {
                messageTextBox.Text = string.Empty;
            }
        }

        private void ItemsControl_SizeChanged(object sender, SizeChangedEventArgs e)
        {
            messageScrollViewer.ScrollToVerticalOffset(messageScrollViewer.ExtentHeight);
        }
    }
}