using System;
using System.Collections.Generic;
using System.Text;
using Xamarin.Forms;

namespace YanSoft.CurrencyExchanger.Core.ControlHelper
{
    public class ListViewIsPullToRefreshEnabled : BindableObject
    {
        public static readonly BindableProperty EnabledProperty =
   BindableProperty.Create("Enabled", typeof(bool), typeof(bool), false, BindingMode.OneWay, null, OnIsPullToRefreshEnabledChanged, null, null, null);

        public bool Enabled
        {
            get => (bool)GetValue(EnabledProperty);
            set => SetValue(EnabledProperty, value);
        }

        static void OnIsPullToRefreshEnabledChanged(BindableObject bindable, object oldValue, object newValue)
        {
            // Property changed implementation goes here
            if (bindable is ListView listView)
            {
                listView.IsPullToRefreshEnabled = (bool)newValue;
            }
        }
    }
}