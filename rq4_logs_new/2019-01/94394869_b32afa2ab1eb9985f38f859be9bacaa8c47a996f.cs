using System;
using System.Collections.Generic;
using System.Text;
using Xamarin.Forms;

namespace YanSoft.CurrencyExchanger.Core.AnimationHelper
{
    public class IsVisibleFadeInAndOutAnimation : BindableObject
    {


        public static readonly BindableProperty IsVisibleProperty =
   BindableProperty.Create("IsVisible", typeof(bool), typeof(bool), true, BindingMode.OneWay, null, OnIsVisibleChangedAsync, null, null, null);

        public bool IsVisible
        {
            get => (bool)GetValue(IsVisibleProperty);
            set => SetValue(IsVisibleProperty, value);
        }

        public static uint? Duration { get; set; }

        static async void OnIsVisibleChangedAsync(BindableObject bindable, object oldValue, object newValue)
        {
            // Property changed implementation goes here
            bool isvisible = (bool)newValue;
            if (bindable is View view)
            {
                if (isvisible)
                {
                    view.IsVisible = true;
                    await view.FadeTo(1, Duration ?? 250);
                } 
                else
                {
                    await view.FadeTo(0, Duration ?? 250);
                    view.IsVisible = false;
                }
            }
        }
    }
}