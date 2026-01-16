using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;
using Windows.System;
using Windows.UI.Xaml;

namespace FootballClient.UWP.ControlExtensions
{

    public enum KeyBehavior
    {
        Down,
        Up
    }

    public class KeyListener : DependencyObject
    {


        public ICommand Command
        {
            get { return (ICommand)GetValue(CommandProperty); }
            set { SetValue(CommandProperty, value); }
        }

        // Using a DependencyProperty as the backing store for Command.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty CommandProperty =
            DependencyProperty.Register("Command", typeof(ICommand), typeof(KeyListener), new PropertyMetadata(null));


        public object CommandParameter { get; set; }

        public KeyBehavior KeyBehavior { get; set; }

        public VirtualKey Key { get; set; }
    }

    public class KeyListenerCollection : List<KeyListener>
    {
        public static KeyListenerCollection Empty = new KeyListenerCollection();
    }

    public class KeyListenerExtension
    {



        public static KeyListenerCollection GetKeyActions(DependencyObject obj)
        {
            return (KeyListenerCollection)obj.GetValue(KeyActionsProperty);
        }

        public static void SetKeyActions(DependencyObject obj, KeyListenerCollection value)
        {
            obj.SetValue(KeyActionsProperty, value);
        }

        public static readonly DependencyProperty KeyActionsProperty =
            DependencyProperty.RegisterAttached("KeyActions", typeof(KeyListenerCollection), typeof(KeyListenerExtension), new PropertyMetadata(KeyListenerCollection.Empty, OnPropertyChanged));

        private static void OnPropertyChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            var uiElement = d as UIElement;
            if (uiElement != null)
            {
                uiElement.KeyDown += UiElement_KeyDown;
                uiElement.KeyUp += UiElement_KeyUp;
            }
        }

        private static void UiElement_KeyUp(object sender, Windows.UI.Xaml.Input.KeyRoutedEventArgs e)
        {
            var actions = GetKeyActions((DependencyObject)sender);
            foreach (var item in actions.Where(x => x.KeyBehavior == KeyBehavior.Up && x.Key == e.Key))
            {
                if (item.Command?.CanExecute(item.CommandParameter) ?? false)
                {
                    item.Command?.Execute(item.CommandParameter);
                }
            }
        }

        private static void UiElement_KeyDown(object sender, Windows.UI.Xaml.Input.KeyRoutedEventArgs e)
        {
            var actions = GetKeyActions((DependencyObject)sender);
            foreach (var item in actions.Where(x => x.KeyBehavior == KeyBehavior.Down && x.Key == e.Key))
            {
                if (item.Command?.CanExecute(item.CommandParameter) ?? false)
                {
                    item.Command?.Execute(item.CommandParameter);
                }
            }
        }
    }
}