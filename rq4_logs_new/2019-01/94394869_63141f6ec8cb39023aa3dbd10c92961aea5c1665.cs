using System;
using System.Collections.Generic;
using System.Text;
using Xamarin.Essentials;
using YanSoft.CurrencyExchanger.Core.Utils;

namespace YanSoft.CurrencyExchanger.Core.Common
{
    public class AppSettings
    {
        public void SetValue<T>(string key, T value)
        {
            try
            {
                Preferences.Set(key, SerializerHelper.ToJson<T>(value));
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

        }

        public T GetValue<T>(string key, T defaultValue)
        {
            T value;
            if (Preferences.ContainsKey(key))
            {
                value = SerializerHelper.FromJson<T>(Preferences.Get(key, defaultValue.ToString()).ToString());
            }
            else
            {
                value = defaultValue;
            }
            return value;

        }

        #region IsAutoRefreshRatesOnStartup
        const string IsAutoRefreshRatesOnStartupKeyName = "IsAutoRefreshRatesOnStartup";
        const bool IsAutoRefreshRatesOnStartupDefault = false;
        public bool IsAutoRefreshRatesOnStartup
        {
            get => Preferences.Get(IsAutoRefreshRatesOnStartupKeyName, IsAutoRefreshRatesOnStartupDefault);
            set => Preferences.Set(IsAutoRefreshRatesOnStartupKeyName, value);
        }
        #endregion

        #region DecimalPrecision
        const string DecimalPrecisionKeyName = "DecimalPrecision";
        const int DecimalPrecisionDefault = 2;
        public int DecimalPrecision
        {
            get => Preferences.Get(DecimalPrecisionKeyName, DecimalPrecisionDefault);
            set => Preferences.Set(DecimalPrecisionKeyName, value);
        }
        #endregion

        #region IsLocalizedCurrencySymbol
        const string IsLocalizedCurrencySymbolKeyName = "IsLocalizedCurrencySymbol";
        const bool IsLocalizedCurrencySymbolDefault = false;
        public bool IsLocalizedCurrencySymbol
        {
            get => Preferences.Get(IsLocalizedCurrencySymbolKeyName, IsLocalizedCurrencySymbolDefault);
            set => Preferences.Set(IsLocalizedCurrencySymbolKeyName, value);
        }
        #endregion

        #region IsEnablePullToRefresh
        const string IsEnablePullToRefreshKeyName = "IsEnablePullToRefresh";
        const bool IsEnablePullToRefreshDefault = false;
        public bool IsEnablePullToRefresh
        {
            get => Preferences.Get(IsEnablePullToRefreshKeyName, IsEnablePullToRefreshDefault);
            set => Preferences.Set(IsEnablePullToRefreshKeyName, value);
        }
        #endregion


        #region IsEnableAutoInitializeToZero
        const string IsEnableAutoInitializeToZeroKeyName = "IsEnableAutoInitialize0";
        const bool IsEnableAutoInitializeToZeroDefault = true;
        public bool IsEnableAutoInitializeToZero
        {
            get => Preferences.Get(IsEnableAutoInitializeToZeroKeyName, IsEnableAutoInitializeToZeroDefault);
            set => Preferences.Set(IsEnableAutoInitializeToZeroKeyName, value);
        }
        #endregion

        #region LanguageCode
        const string LanguageCodeKeyName = "LanguageCode";
        const string LanguageCodeDefault = "";
        public string LanguageCode
        {
            get => Preferences.Get(LanguageCodeKeyName, LanguageCodeDefault);
            set => Preferences.Set(LanguageCodeKeyName, value);
        }
        #endregion

    }
}