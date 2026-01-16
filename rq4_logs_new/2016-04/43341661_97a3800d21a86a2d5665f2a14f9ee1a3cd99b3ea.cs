using System;
using Xamarin.Forms;

namespace DebtCalculator.Shared
{
  public class CustomNavigationPage : NavigationPage
  {
    string _barItemFontFamily = "FontAwesome";
    int _barItemFontSize = 16;

    public CustomNavigationPage(Page page) : base(page)
    {
    }
      
    public string BarItemFontFamily 
    {
      get { return _barItemFontFamily; }
      set
      {
        _barItemFontFamily = value;
      }
    }

    public int BarItemFontSize 
    {
      get { return _barItemFontSize; }
      set
      {
        _barItemFontSize = value;
      }
    }

    public string BarTitleFontFamily 
    {
      get { return _barItemFontFamily; }
      set
      {
        _barItemFontFamily = value;
      }
    }

    public int BarTitleFontSize 
    {
      get { return _barItemFontSize; }
      set
      {
        _barItemFontSize = value;
      }
    }
  }
}
