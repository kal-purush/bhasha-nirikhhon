using System;
using System.Collections.Generic;
using System.Text;
using Xamarin.Forms;

namespace FormsMvvm2019
{
    static class BizLogic
    {
        static public Command Command => new Command((args) =>
        {
            switch (args)
            {
                case MainViewModel mainViewModel:
                    Application.Current.MainPage.DisplayAlert("BizLogic", $"PassCode={mainViewModel.PassCode}", "OK");
                    break;

                default:
                    Application.Current.MainPage.DisplayAlert("BizLogic", $"unknown args={args}", "OK");
                    break;
            }
        });
    }
}