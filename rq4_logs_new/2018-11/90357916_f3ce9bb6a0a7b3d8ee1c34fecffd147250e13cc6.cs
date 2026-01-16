using System.Globalization;
using MvvmCross.UI;
using System;
using MvvmCross.Plugin.Color;
using System.Diagnostics;

namespace LevelUp.Xamarin.OpenWeather.App.Shared.Converters
{
    public class AlernatingRowColorValueConverter : MvxColorValueConverter<bool>
	{
        protected override MvxColor Convert(bool value, object parameter, CultureInfo culture)
        {
            Debug.WriteLine("AlernatingRowColorValueConverter: " + value);
            return value ? new MvxColor(0): new MvxColor(80, 80, 80, 50);
        }
    }
}