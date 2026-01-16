using System.Windows;

namespace HighFreqUpdate.Views
{
    public partial class ManagePropertiesView
    {
        public ManagePropertiesView()
        {
            InitializeComponent();
        }

        private void ManageNumberOptionsVisibility(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (sender is FrameworkElement usercontrol)
            {
                aggregatorRow.Height = usercontrol.IsVisible ? GridLength.Auto : new GridLength(0);
                decimalsRow.Height = usercontrol.IsVisible ? GridLength.Auto : new GridLength(0);
                thousandSeparatorRow.Height = usercontrol.IsVisible ? GridLength.Auto : new GridLength(0);
            }
        }

        private void ManageDateOptionsVisibility(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (sender is FrameworkElement usercontrol)
            {
                formatRow.Height = usercontrol.IsVisible ? new GridLength(35) : new GridLength(0);
            }
        }

        private void ManageBlinkVisibility(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (sender is FrameworkElement usercontrol)
            {
                blinkRow.Height = usercontrol.IsVisible ? new GridLength(35) : new GridLength(0);
                blinkTimeRow.Height = usercontrol.IsVisible ? new GridLength(35) : new GridLength(0);
            }
        }

        private void ManageBlinkColorVisibility(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (sender is FrameworkElement usercontrol)
            {
                blinkColorRow.Height = usercontrol.IsVisible ? new GridLength(35) : new GridLength(0);
            }
        }

        private void ManageForeColoreNegativeVisibility(object sender, DependencyPropertyChangedEventArgs e)
        {
            if (sender is FrameworkElement usercontrol)
            {
                fontNegativeRow.Height = usercontrol.IsVisible ? new GridLength(35) : new GridLength(0);
                fontColorNegativeRow.Height = usercontrol.IsVisible ? new GridLength(35) : new GridLength(0);
            }
        }
    }
}