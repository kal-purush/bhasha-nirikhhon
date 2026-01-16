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

namespace LanguageSchoolApp.view
{
    /// <summary>
    /// Interaction logic for PopupMessageView.xaml
    /// </summary>
    public partial class PopupMessageView : Window
    {
        public PopupMessageView(string messageType, string message)
        {
            InitializeComponent();
            Label titleLabel = (Label)FindName("TitleLabel");
            titleLabel.Content = messageType;
            TextBlock messageTextBlock = (TextBlock)FindName("MessageContentTB");
            messageTextBlock.Text = message;

            Border border = (Border)FindName("RectangleBorder");
            if (messageType == "SUCCESS")
            {
                border.Background = new SolidColorBrush(Color.FromRgb(118,208,124));
            }
            else if (messageType == "ERROR")
            {
                border.Background = new SolidColorBrush(Color.FromRgb(255,87,87));
            }

            CloseAfterDelayAsync();
        }

        private async void CloseAfterDelayAsync()
        {
            await Task.Delay(3000);
            this.Close();
        }
    }
}