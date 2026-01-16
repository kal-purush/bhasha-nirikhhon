using System;
using System.Dynamic;
using System.Windows.Data;
using Infragistics.Windows.DockManager;

namespace DockingInfragistics
{
    using System.Windows;
    using System.Windows.Controls;
    using Catel.ApiCop;
    using Catel.ApiCop.Listeners;
    using Catel.IoC;
    using Catel.Logging;
    using Catel.Reflection;
    using Catel.Windows;
    using IF.WPF.Infragistics.Persistence.Extensions;
    using IF.WPF.Infragistics.Persistence.Services.Interfaces;

    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        protected override void OnStartup(StartupEventArgs e)
        {
#if DEBUG
            LogManager.AddDebugListener();
#endif

            Log.Info("Starting application");

            // Want to improve performance? Uncomment the lines below. Note though that this will disable
            // some features. 
            //
            // For more information, see http://docs.catelproject.com/vnext/faq/performance-considerations/

            // Log.Info("Improving performance");
            // Catel.Windows.Controls.UserControl.DefaultCreateWarningAndErrorValidatorForViewModelValue = false;
            // Catel.Windows.Controls.UserControl.DefaultSkipSearchingForInfoBarMessageControlValue = true;

            // TODO: Register custom types in the ServiceLocator
            //Log.Info("Registering custom types");
            var serviceLocator = ServiceLocator.Default;

            serviceLocator.RegisterType<IDockManagerPersistenceService, DockManagerPersistenceService>();
            // To auto-forward styles, check out Orchestra (see https://github.com/wildgums/orchestra)
            // StyleHelper.CreateStyleForwardersForDefaultStyles();

            Log.Info("Calling base.OnStartup");

            base.OnStartup(e);
        }

        protected override void OnExit(ExitEventArgs e)
        {
            // Get advisory report in console
            ApiCopManager.AddListener(new ConsoleApiCopListener());
            ApiCopManager.WriteResults();

            base.OnExit(e);
        }


        private void FrameworkElement_OnLoaded(object sender, RoutedEventArgs e)
        {
            //var frameworkElement = sender as FrameworkElement;

            
            //    var pane = frameworkElement.ParentOfType<Grid>();

            //    frameworkElement.DataContext = pane?.DataContext;

            //    var header = pane.ParentOfType<ContentPane>();

            //    if (header != null)
            //    {
            //        try
            //        {
            //            BindingOperations.ClearAllBindings(header);
            //            header.Header = pane.DataContext;

            //    }
            //        catch (Exception exception)
            //        {
            //            Console.WriteLine(exception);
                        
            //        }
                    
            //        //header.DataContext = frameworkElement.DataContext;
            //    }
          
        }

    

        private void FrameworkElement_OnDataContextChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            int r = 0;
        }
    }
}