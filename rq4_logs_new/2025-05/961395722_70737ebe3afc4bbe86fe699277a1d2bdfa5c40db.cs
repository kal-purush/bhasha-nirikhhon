using System.Windows.Input;
using KaraIOke.Services.Navigation;

namespace KaraIOke.ViewModels;

public partial class PlayerViewModel
{
    protected readonly NavigationService _navigationService;

    public PlayerViewModel(IServiceProvider serviceProvider) {
        _navigationService = serviceProvider.GetService<NavigationService>();

        GoBack = new Command(
            execute: async () => {
                await _navigationService.PopPage();
            }
        );
    }

    public ICommand GoBack { private set; get; }
}