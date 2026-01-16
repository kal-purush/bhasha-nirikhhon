using System;
using System.Collections.Generic;
using System.Text;
using MvvmCross.ViewModels;

namespace YanSoft.CurrencyExchanger.Core.Models
{
    public class CurrencyExchangeBindableItem : MvxViewModel
    {
        public Guid Id { get; set; }


        #region SourceCurrency;
        private CurrencyItem _sourceCurrency;
        public CurrencyItem SourceCurrency
        {
            get => _sourceCurrency;
            set => SetProperty(ref _sourceCurrency, value);
        }
        #endregion


        #region SourceCode;
        private string _sourceCode;
        public string SourceCode
        {
            get => _sourceCode;
            set => SetProperty(ref _sourceCode, value);
        }
        #endregion


        #region TargetCurrency;
        private CurrencyItem _targetCurrency;
        public CurrencyItem TargetCurrency
        {
            get => _targetCurrency;
            set => SetProperty(ref _targetCurrency, value);
        }
        #endregion


        #region TargetCode;
        private string _targetCode;
        public string TargetCode
        {
            get => _targetCode;
            set => SetProperty(ref _targetCode, value);
        }
        #endregion


        #region Rate;
        private decimal _rate;
        public decimal Rate
        {
            get => _rate;
            set => SetProperty(ref _rate, value);
        }
        #endregion


        #region Amount;
        private decimal _amount;
        public decimal Amount
        {
            get => _amount;
            set => SetProperty(ref _amount, value);
        }
        #endregion


        #region AmountText;
        private string _amountText;
        public string AmountText
        {
            get => _amountText;
            set => SetProperty(ref _amountText, value);
        }
        #endregion


        #region SortOrder;
        private int _sortOrder;
        public int SortOrder
        {
            get => _sortOrder;
            set => SetProperty(ref _sortOrder, value);
        }
        #endregion


        #region IsStandard;
        private bool _isStandard;
        public bool IsStandard
        {
            get => _isStandard;
            set => SetProperty(ref _isStandard, value);
        }
        #endregion


        #region UpdateTimeUtc;
        private DateTime _updateTimeUtc;
        public DateTime UpdateTimeUtc
        {
            get => _updateTimeUtc;
            set => SetProperty(ref _updateTimeUtc, value);
        }
        #endregion
    }
}