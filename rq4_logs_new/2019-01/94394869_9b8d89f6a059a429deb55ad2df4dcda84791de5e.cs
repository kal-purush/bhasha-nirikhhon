using System;
using System.Collections.Generic;
using System.Text;
using MvvmCross.Navigation;
using YanSoft.CurrencyExchanger.Core.Common;

namespace YanSoft.CurrencyExchanger.Core.ViewModels
{
    public class SettingsViewModel : BaseViewModel
    {
        private readonly IMvxNavigationService _navigationService;
        private readonly AppSettings _appSettings;
        public SettingsViewModel(IMvxNavigationService navigationService, AppSettings appSettings)
        {
            _navigationService = navigationService;
            _appSettings = appSettings;
        }



        #region IsAutoRefreshRatesOnStartup;
        private bool _isAutoRefreshRatesOnStartup;
        public bool IsAutoRefreshRatesOnStartup
        {
            get
            {
                _isAutoRefreshRatesOnStartup = _appSettings.IsAutoRefreshRatesOnStartup;
                return _isAutoRefreshRatesOnStartup;
            }
            set
            {
                SetProperty(ref _isAutoRefreshRatesOnStartup, value);
                _appSettings.IsAutoRefreshRatesOnStartup = value;
            }
        }
        #endregion


        #region IsEnablePullToRefresh;
        private bool _isEnablePullToRefresh;
        public bool IsEnablePullToRefresh
        {
            get
            {
                _isEnablePullToRefresh = _appSettings.IsEnablePullToRefresh;
                return _isEnablePullToRefresh;
            }
            set
            {
                SetProperty(ref _isEnablePullToRefresh, value);
                _appSettings.IsEnablePullToRefresh = value;
            }
        }
        #endregion


        #region DecimalPrecision;
        private int _decimalPrecision;
        public int DecimalPrecision
        {
            get
            {
                _decimalPrecision = _appSettings.DecimalPrecision;
                return _decimalPrecision;
            }
            set
            {
                SetProperty(ref _decimalPrecision, value);
                _appSettings.DecimalPrecision = value;
            }
        }
        #endregion


        #region IsEnableLocalizedCurrencySymbol;
        private bool _isEnableLocalizedCurrencySymbol;
        public bool IsEnableLocalizedCurrencySymbol
        {
            get
            {
                _isEnableLocalizedCurrencySymbol = _appSettings.IsEnableLocalizedCurrencySymbol;
                return _isEnableLocalizedCurrencySymbol;
            }
            set
            {
                SetProperty(ref _isEnableLocalizedCurrencySymbol, value);
                _appSettings.IsEnableLocalizedCurrencySymbol = value;
            }
        }
        #endregion


        #region IsEnableAutoInitializeToZero;
        private bool _isEnableAutoInitializeToZero;
        public bool IsEnableAutoInitializeToZero
        {
            get
            {
                _isEnableAutoInitializeToZero = _appSettings.IsEnableAutoInitializeToZero;
                return _isEnableAutoInitializeToZero;
            }
            set
            {
                SetProperty(ref _isEnableAutoInitializeToZero, value);
                _appSettings.IsEnableAutoInitializeToZero = value;
            }
        }
        #endregion


    }
}