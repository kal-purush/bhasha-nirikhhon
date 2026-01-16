using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Threading.Tasks;
using MvvmCross.Commands;
using MvvmCross.Navigation;
using YanSoft.CurrencyExchanger.Core.Common;
using YanSoft.CurrencyExchanger.Core.Models;
using YanSoft.CurrencyExchanger.Core.Services;

namespace YanSoft.CurrencyExchanger.Core.ViewModels
{
    public class EditListViewModel : BaseViewModel<ObservableCollection<CurrencyExchangeBindableItem>>
    {
        private readonly IMvxNavigationService _navigationService;
        private readonly ICurrencyService _currencyService;
        private readonly GlobalContext _globalContext;
        private readonly IDataService<CurrencyExchangeItem> _dataService;
        public EditListViewModel(IMvxNavigationService navigationService,
            ICurrencyService currencyService, GlobalContext globalContext,
            IDataService<CurrencyExchangeItem> dataService)
        {
            _navigationService = navigationService;
            _currencyService = currencyService;
            _globalContext = globalContext;
            _dataService = dataService;
        }

        #region Properties
        #region CurrencyList;
        private ObservableCollection<CurrencyExchangeBindableItem> _currencyList;
        public ObservableCollection<CurrencyExchangeBindableItem> CurrencyList
        {
            get => _currencyList;
            set => SetProperty(ref _currencyList, value);
        }
        #endregion

        #region Lifecycle events
        public override void Prepare(ObservableCollection<CurrencyExchangeBindableItem> list)
        {
            base.Prepare();
            CurrencyList = list;
        }
        #endregion

        #endregion

        #region Commands

        //#region SetBaseCurrencyAsyncCommand;
        //private IMvxAsyncCommand<CurrencyExchangeBindableItem> _setBaseCurrencyAsyncCommand;
        //public IMvxAsyncCommand<CurrencyExchangeBindableItem> SetBaseCurrencyAsyncCommand
        //{
        //    get
        //    {
        //        _setBaseCurrencyAsyncCommand = _setBaseCurrencyAsyncCommand ?? new MvxAsyncCommand<CurrencyExchangeBindableItem>(SetBaseCurrencyAsync);
        //        return _setBaseCurrencyAsyncCommand;
        //    }
        //}
        //private async Task SetBaseCurrencyAsync(CurrencyExchangeBindableItem param)
        //{
        //    // Implement your logic here.
        //    _globalContext.CurrentBaseCurrency = param;
        //    //IsRefreshing = true;

        //    _currencyService.SetBaseCurrency(CurrencyList, param);
        //    await GetLatestRatesAsync();
        //    //if (IsRefreshing)
        //    //{
        //    //    IsRefreshing = false;
        //    //}


        //}
        //#endregion

        #region DeleteCurrencyAsyncCommand;
        private IMvxAsyncCommand<CurrencyExchangeBindableItem> _deleteCurrencyAsyncCommand;
        public IMvxAsyncCommand<CurrencyExchangeBindableItem> DeleteCurrencyAsyncCommand
        {
            get
            {
                _deleteCurrencyAsyncCommand = _deleteCurrencyAsyncCommand ?? new MvxAsyncCommand<CurrencyExchangeBindableItem>(DeleteCurrencyAsync);
                return _deleteCurrencyAsyncCommand;
            }
        }
        private async Task DeleteCurrencyAsync(CurrencyExchangeBindableItem param)
        {
            // Implement your logic here.
            if (!param.IsBaseCurrency)
            {
                await _currencyService.DeleteCurrencyAsync(CurrencyList, param);
            }
        }
        #endregion


        #endregion
    }
}