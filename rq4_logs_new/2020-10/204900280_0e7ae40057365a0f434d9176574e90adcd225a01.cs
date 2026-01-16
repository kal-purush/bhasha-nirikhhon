using System.Reactive.Disposables;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Pixama.App.Configuration;
using Pixama.Logic.ViewModels.Pages;
using ReactiveUI;

namespace Pixama.App.Views.Pages
{
    public sealed partial class SourcePage : Page, IViewFor<SourcePageViewModel>
    {
        public SourcePage()
        {
            ViewModel = ServiceLocator.Current.GetService<SourcePageViewModel>();
            this.InitializeComponent();
            this.Loading += OnLoading;
            this.Unloaded += OnUnloaded;
            this.WhenActivated(disposable =>
            {
                this.OneWayBind(ViewModel,
                        vm => vm.SourceFolders,
                        v => v.Folders.ItemsSource)
                    .DisposeWith(disposable);
            });
        }

        private async void OnLoading(FrameworkElement sender, object args)
        {
            await this.ViewModel.LoadFoldersAsync();
        }

        private void OnUnloaded(object sender, RoutedEventArgs e)
        {
            throw new System.NotImplementedException();
        }

        object IViewFor.ViewModel
        {
            get => ViewModel;
            set => ViewModel = (SourcePageViewModel)value;
        }

        public SourcePageViewModel ViewModel { get; set; }
    }
}