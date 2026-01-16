using System.Windows;

namespace Desktop.Extensions.Properties
{
  class ReturnDialogResult
  {
    public static readonly DependencyProperty DialogResultProperty =
      DependencyProperty.RegisterAttached(
      "DialogResult",
      typeof(bool?),
      typeof(ReturnDialogResult),
      new PropertyMetadata(DialogResultChanged));

    private static void DialogResultChanged(DependencyObject dObject, DependencyPropertyChangedEventArgs e)
    {
      Window window = dObject as Window;
      if (window != null)
        window.DialogResult = e.NewValue as bool?;
    }

    public static void SetDialogResult(Window target, bool? value)
    {
      target.SetValue(DialogResultProperty, value);
    }
  }
}