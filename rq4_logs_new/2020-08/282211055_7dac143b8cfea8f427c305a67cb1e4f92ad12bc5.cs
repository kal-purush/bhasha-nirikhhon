using System.Collections;
using System.Windows;
using System.Windows.Controls;

namespace Desktop.Extensions.Controls
{
  /// <summary>
  /// Interaction logic for LabelledComboBox.xaml
  /// </summary>
  public partial class LabelledComboBox : UserControl
  {
    public LabelledComboBox()
    {
      InitializeComponent();
      Root.DataContext = this;
    }

    public static readonly DependencyProperty LabelProperty =
      DependencyProperty.Register("Label",
                                  typeof(string),
                                  typeof(LabelledComboBox),
                                  new FrameworkPropertyMetadata("Unnamed Label"));

    public static readonly DependencyProperty ItemSourceProperty =
      DependencyProperty.Register("ItemSource",
                                  typeof(IEnumerable),
                                  typeof(LabelledComboBox),
                                  new FrameworkPropertyMetadata("", FrameworkPropertyMetadataOptions.BindsTwoWayByDefault));

    public static readonly DependencyProperty SelectedItemProperty =
      DependencyProperty.Register("SelectedItem",
                                  typeof(object),
                                  typeof(LabelledComboBox),
                                  new PropertyMetadata(null));

    public string Label
    {
      get { return (string)GetValue(LabelProperty); }
      set { SetValue(LabelProperty, value); }
    }

    public IEnumerable ItemSource
    {
      get { return (IEnumerable)GetValue(ItemSourceProperty); }
      set { SetValue(ItemSourceProperty, value); }
    }

    public object SelectedItem
    {
      get { return GetValue(SelectedItemProperty); }
      set { SetValue(SelectedItemProperty, value); }
    }
  }
}