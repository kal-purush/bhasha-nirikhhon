using System.Windows;

namespace Spicy
{
    public class Extensions
    {
        public static readonly DependencyProperty SoundProperty =
            DependencyProperty.RegisterAttached("Sound", typeof(Sound), typeof(Extensions), new PropertyMetadata(default(Sound)));

        public static void SetSound(UIElement element, Sound sound)
        {
            element.SetValue(SoundProperty, sound);
        }

        public static Sound GetSound(UIElement element)
        {
            return (Sound)element.GetValue(SoundProperty);
        }
    }
}