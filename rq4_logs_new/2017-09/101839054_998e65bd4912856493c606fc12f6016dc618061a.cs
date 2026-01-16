using System.Reflection;
using System.Windows;
using Autofac;
using FotoSortierer_v2.Helper;
using FotoSortierer_v2.Helper.Builder;
using FotoSortierer_v2.Services;
using Helper.Builder;
using MVVM.Messenger;
using Ookii.Dialogs.Wpf;
using Repository;

namespace FotoSortierer_v2
{
    /// <summary>
    /// Interaktionslogik für "App.xaml"
    /// </summary>
    public partial class App : Application
    {
        private IContainer _appContainer;

        protected override void OnStartup(StartupEventArgs e)
        {
            // Initialize container
            SetupContainer();

            // Set private field Container in seperate singleton class.
            typeof(IocContainer).GetField("_container", BindingFlags.Instance | BindingFlags.NonPublic)?.SetValue(IocContainer.Instance, _appContainer);

            base.OnStartup(e);
        }

        private void SetupContainer()
        {
            var builder = new ContainerBuilder();

            // register instance of Messenger as its implemented interface as a singleton class.
            builder.RegisterType<Messenger>().As<IMessenger>().SingleInstance();

            // register Dialogs
            builder.RegisterType<VistaFolderBrowserDialog>().InstancePerLifetimeScope();
            builder.RegisterType<VistaOpenFileDialog>().WithProperty("Multiselect", true).InstancePerLifetimeScope();

            // register Helper
            builder.RegisterType<CameraModelBuilder>().AsImplementedInterfaces().SingleInstance();
            builder.RegisterType<PhotoModelBuilder>().AsImplementedInterfaces().SingleInstance();
            builder.RegisterType<CameraViewModelBuilder>().AsImplementedInterfaces().SingleInstance();
            builder.RegisterType<PhotoViewModelBuilder>().AsImplementedInterfaces().SingleInstance();

            // register Repositories
            builder.RegisterType<PhotoRepository>().AsImplementedInterfaces().SingleInstance();

            // register Services
            builder.RegisterType<DialogsService>().AsImplementedInterfaces().SingleInstance();
            builder.RegisterType<PhotoService>().AsImplementedInterfaces().SingleInstance();

            // register all viewmodels as its implemented interfaces, except the mockviewmodel.
            builder.RegisterAssemblyTypes(Assembly.GetExecutingAssembly()).Where(t => t.Name.EndsWith("ViewModel") && !t.Name.StartsWith("Mock")).AsImplementedInterfaces();

            _appContainer = builder.Build();
        }
    }
}