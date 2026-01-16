using System.Windows;
using System.Windows.Controls;

namespace Desktop.Extensions.Properties
{
  class StretchControl : DependencyObject
  {
    public static readonly DependencyProperty ResizeOnStartProperty =
      DependencyProperty.RegisterAttached("ResizeOnStart",
        typeof(bool),
        typeof(StretchControl),
        new PropertyMetadata(ResizeOnStartChanged));

    public static readonly DependencyProperty ResizeTriggerProperty =
      DependencyProperty.RegisterAttached("ResizeTrigger",
        typeof(bool),
        typeof(StretchControl),
        new PropertyMetadata(ResizeTriggerChanged));

    private static void ResizeOnStartChanged(DependencyObject obj, DependencyPropertyChangedEventArgs e)
    {
      ResizeTriggerChanged(obj, e);
    }

    private static void ResizeTriggerChanged(DependencyObject obj, DependencyPropertyChangedEventArgs e)
    {
      FrameworkElement element = obj as FrameworkElement;
      Panel parentPanel = element.Parent as Panel;
      double heightRemaining = parentPanel.Height;

      foreach (FrameworkElement childElement in parentPanel.Children)
      {
        if (element.Name != childElement.Name && childElement.Visibility != Visibility.Collapsed)
        {
          heightRemaining -= childElement.Height; 
        }
      }

      element.Height = heightRemaining;
    }

    public static bool GetResizeOnStart(DependencyObject obj)
    {
      return (bool)obj.GetValue(ResizeOnStartProperty);
    }

    public static void SetResizeOnStart(DependencyObject obj, bool value)
    {
      obj.SetValue(ResizeOnStartProperty, value);
    }

    public static bool GetResizeTrigger(DependencyObject obj)
    {
      return (bool)obj.GetValue(ResizeTriggerProperty);
    }

    public static void SetResizeTrigger(DependencyObject obj, bool value)
    {
      obj.SetValue(ResizeTriggerProperty, value);
    }
  }
}