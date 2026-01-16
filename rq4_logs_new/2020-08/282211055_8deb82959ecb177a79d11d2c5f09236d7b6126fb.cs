using System.Collections;
using System.Windows;
using System.Windows.Controls;

namespace Desktop.Extensions.Controls
{
  /// <summary>
  /// Interaction logic for SearchTextBox.xaml
  /// </summary>
  public partial class SearchTextBox : UserControl
  {
    public SearchTextBox()
    {
      InitializeComponent();
      Root.DataContext = this;
    }

    public static readonly DependencyProperty SearchProperty =
      DependencyProperty.Register("SearchForText",
                                  typeof(string),
                                  typeof(SearchTextBox));

    public string SearchForText
    {
      get { return (string)GetValue(SearchProperty); }
      set { SetValue(SearchProperty, value); }
    }
  }
}