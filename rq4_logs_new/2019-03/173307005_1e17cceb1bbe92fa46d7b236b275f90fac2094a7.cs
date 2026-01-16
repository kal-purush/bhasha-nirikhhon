using System;
using Xamarin.Forms;

namespace XamarinForms.Controls.CheckBoxes
{
    public class CheckBox : ContentView
    {
        public CheckBox()
        {
            Padding = new Thickness(0);
            Margin = new Thickness(40, 20, 40, 20);

            var checBoxLabel = new Label()
            {
                Text = "\u2610",
                FontFamily = "Arial",
                FontSize = 20,
                TextColor = Color.FromHex("#7f8082")
            };

            var tapGestureRecognizer = new TapGestureRecognizer();
            tapGestureRecognizer.Tapped += (object sender, EventArgs args) =>
            checBoxLabel.Text = checBoxLabel.Text == "\u2611"
            ? "\u2610"
            : "\u2611";

            StackLayout stackLayout = new StackLayout()
            {
                Orientation = StackOrientation.Horizontal,
                HorizontalOptions = LayoutOptions.StartAndExpand,
                Children = { checBoxLabel }
            };

            stackLayout.GestureRecognizers.Add(tapGestureRecognizer);

            Content = stackLayout;
        }
    }
}