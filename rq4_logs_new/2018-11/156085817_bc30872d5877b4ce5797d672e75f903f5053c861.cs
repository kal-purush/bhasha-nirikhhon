using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using Tobii.Interaction;
using Tobii.Interaction.Wpf;

namespace TobiiTest
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        private Host _host;
        private WpfInteractorAgent _wpfInteractorAgent;

        protected override void OnStartup(StartupEventArgs e)
        {
            _host = new Host();
            _wpfInteractorAgent = _host.InitializeWpfAgent();
        }

        protected override void OnExit(ExitEventArgs e)
        {
            _host.Dispose();
            base.OnExit(e);
        }

        public Host Host
        {
            get { return _host; }
        }
    }
}