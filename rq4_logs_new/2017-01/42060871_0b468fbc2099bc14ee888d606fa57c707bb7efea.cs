using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Threading;

namespace lab5
{
    /// <summary>
    /// Логика взаимодействия для App.xaml
    /// </summary>
    public partial class App : Application
    {
        /*private void OnStartup(object sender, StartupEventArgs e)
        {
            MainWindow view = new MainWindow();
            view.DataContext = new MainViewModel();
            view.Show();
        }*/

        private void GlobalExceptionHandler(object sender,
            DispatcherUnhandledExceptionEventArgs e)
        {
            MessageBox.Show(e.Exception.Message);
            e.Handled = true;
        }
    }
}