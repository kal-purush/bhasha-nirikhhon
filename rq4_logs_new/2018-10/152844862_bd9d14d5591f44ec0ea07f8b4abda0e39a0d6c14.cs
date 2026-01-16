using MahApps.Metro;
using MahApps.Metro.Controls.Dialogs;
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

namespace AirNewUI
{
    /// <summary>
    /// LogIn.xaml 的互動邏輯
    /// </summary>
    public partial class LogInWindow
    {
        public LogInWindow()
        {
            InitializeComponent();
            ThemeManager.ChangeAppStyle(this,
                            ThemeManager.GetAccent("Steel"),
                            ThemeManager.GetAppTheme("BaseLight"));
        }

        AirEntities dbContext = new AirEntities();

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            this.ShowTitleBar = false;
            this.BorderThickness = new Thickness(0);
        }

        private void MetroWindow_MouseDown(object sender, MouseButtonEventArgs e)
        {
            if (e.ChangedButton == MouseButton.Left)
                this.DragMove();
        }
    }
}