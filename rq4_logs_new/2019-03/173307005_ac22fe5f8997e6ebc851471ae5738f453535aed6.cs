using System;
using Xamarin.Forms;

namespace XamarinForms.Controls.RadioButtons
{
    public class LabelledRadioButton : ContentView
    {
        public LabelledRadioButton(
            string label,
            double fontSize = 20,
            string fontFamily = "Arial",
            FontAttributes fontAttributes = FontAttributes.None,
            Color? textColor = null)
        {
            Padding = new Thickness(0);
            Margin = new Thickness(40, 20, 40, 20);

            var checBoxLabel = new Label()
            {
                Text = "\u25CB",
                FontFamily = fontFamily,
                FontSize = fontSize * 2,
                TextColor = Color.FromHex("#7f8082")
            };

            var labelText = new Label
            {
                Text = label,
                FontFamily = fontFamily,
                FontSize = fontSize,
                FontAttributes = fontAttributes,
                TextColor = textColor ?? Color.Black,
                VerticalTextAlignment = TextAlignment.Center,
                Margin = new Thickness(10, 0, 10, 0)
            };

            var tapGestureRecognizer = new TapGestureRecognizer();
            tapGestureRecognizer.Tapped += (object sender, EventArgs args) =>
            checBoxLabel.Text = checBoxLabel.Text == "\u25CB"
            ? "\u25CF"
            : "\u25CB";

            StackLayout stackLayout = new StackLayout()
            {
                Orientation = StackOrientation.Horizontal,
                HorizontalOptions = LayoutOptions.StartAndExpand,
                Children = { checBoxLabel, labelText }
            };

            stackLayout.GestureRecognizers.Add(tapGestureRecognizer);

            Content = stackLayout;
        }
    }
}