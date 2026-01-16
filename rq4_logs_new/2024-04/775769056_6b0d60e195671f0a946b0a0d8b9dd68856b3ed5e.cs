using LagDaemon.YAMUD.Desltop.ViewModels;
using Microsoft.Extensions.DependencyInjection;
using System.Configuration;
using System.Data;
using System.Windows;

namespace LagDaemon.YAMUD.Desltop
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        private IServiceProvider _serviceProvider;

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

            _serviceProvider = ConfigureServices.Configure().BuildServiceProvider();

            // Use the service provider to create and configure views
            var mainWindowVm = _serviceProvider.GetRequiredService<MainWindowViewModel>();
            var mainWindow = _serviceProvider.GetRequiredService<MainWindow>();
            mainWindow.DataContext = mainWindowVm;
            mainWindow.Show();
        }

 
    }



}