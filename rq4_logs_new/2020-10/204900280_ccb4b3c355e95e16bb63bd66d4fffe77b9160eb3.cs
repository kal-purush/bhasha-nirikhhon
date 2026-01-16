using System.Reactive.Disposables;
using Windows.UI;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Pixama.Logic.ViewModels.Common;
using ReactiveUI;

// The User Control item template is documented at https://go.microsoft.com/fwlink/?LinkId=234236

namespace Pixama.App.Views.Photo
{
    public class DestinationLocationItemViewBase : ReactiveUserControl<DestinationFolderViewModel> { }
    public sealed partial class DestinationLocationItemView : DestinationLocationItemViewBase
    {
        public DestinationLocationItemView()
        {
            InitializeComponent();
            FavoriteButton.PointerEntered += FavoriteButtonOnPointerEntered;
            FavoriteButton.PointerExited += FavoriteButtonOnPointerExited;
            this.WhenActivated(disposable =>
            {
                this.OneWayBind(ViewModel,
                    vm => vm.Name,
                    v => v.FolderName.Text)
                    .DisposeWith(disposable);

                this.BindCommand(ViewModel,
                        vm => vm.FavoriteClickCommand,
                        v => v.FavoriteButton)
                    .DisposeWith(disposable);
            });
        }

        private void FavoriteButtonOnPointerExited(object sender, PointerRoutedEventArgs e)
        {
            FavoriteGlyph.Glyph = "\uE734";
            FavoriteGlyph.Foreground = new SolidColorBrush(Colors.Black);
        }

        private void FavoriteButtonOnPointerEntered(object sender, PointerRoutedEventArgs e)
        {
            FavoriteGlyph.Glyph = "\uE74D";
            FavoriteGlyph.Foreground = new SolidColorBrush(Colors.Red);
        }
    }
}