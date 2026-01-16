using System.Threading.Tasks;
using MvvmCross.Navigation;
using MvvmCross.ViewModels;
using YanSoft.CurrencyExchanger.Core.ViewModels.Home;

namespace YanSoft.CurrencyExchanger.Core.ViewModels
{
    public class MasterDetailViewModel : BaseViewModel
    {
        private readonly IMvxNavigationService _navigationService;

        public MasterDetailViewModel(IMvxNavigationService navigationService)
        {
            _navigationService = navigationService;
        }

        public override void ViewAppearing()
        {
            base.ViewAppearing();
            MvxNotifyTask.Create(async () => await InitializeViewModelsAsync());
        }

        private async Task InitializeViewModelsAsync()
        {
            await _navigationService.Navigate<MenuViewModel>();
            await _navigationService.Navigate<HomeViewModel>();
        }
    }
}