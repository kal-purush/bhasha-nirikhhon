using GalaxyZooTouchTable.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Interactivity;
using System.Windows.Media;
using System.Windows.Media.Animation;

namespace GalaxyZooTouchTable.Behaviors
{
    public class TempDisableBorder : Behavior<UIElement>
    {
        public static readonly DependencyProperty ObservedElementProperty =
            DependencyProperty.Register("ObservedElement", typeof(List<TableSubject>),
                typeof(TempDisableBorder), new FrameworkPropertyMetadata(null, ValueChangedCallback));

        public List<TableSubject> ObservedElement
        {
            get { return (List<TableSubject>)GetValue(ObservedElementProperty); }
            set { SetValue(ObservedElementProperty, value); }
        }

        static async void ValueChangedCallback(DependencyObject sender, DependencyPropertyChangedEventArgs e)
        {
            int SECONDS_DISABLED = 5;

            TempDisableBorder item = sender as TempDisableBorder;
            if (item == null || item.AssociatedObject == null) return;

            Color successColor = (Color)ColorConverter.ConvertFromString("#63AB51");
            Border borderItem = item.AssociatedObject as Border;
            LinearGradientBrush background = new LinearGradientBrush();
            background.StartPoint = new Point(0, 0.5);
            background.EndPoint = new Point(1, 0.5);

            GradientStop firstStop = new GradientStop(successColor, 0.0);
            GradientStop middleStop = new GradientStop(Colors.Gray, 0.0);
            GradientStop lastStop = new GradientStop(Colors.Gray, 1.0);

            background.GradientStops.Add(firstStop);
            background.GradientStops.Add(middleStop);
            background.GradientStops.Add(lastStop);
            borderItem.Background = background;

            DoubleAnimation animate = new DoubleAnimation(0, 1, TimeSpan.FromSeconds(SECONDS_DISABLED));
            ColorAnimation colorAnimation = new ColorAnimation(successColor, TimeSpan.FromSeconds(SECONDS_DISABLED));
            middleStop.BeginAnimation(GradientStop.OffsetProperty, animate);
            middleStop.BeginAnimation(GradientStop.ColorProperty, colorAnimation);

            borderItem.IsHitTestVisible = false;
            await Task.Delay(TimeSpan.FromSeconds(SECONDS_DISABLED));
            borderItem.IsHitTestVisible = true;
        }
    }
}