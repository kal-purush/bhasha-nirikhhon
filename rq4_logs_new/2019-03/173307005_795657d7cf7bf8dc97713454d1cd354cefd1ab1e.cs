using Xamarin.Forms;

namespace XamarinForms.Controls.CheckBoxes
{
    public class LabelledCheckBox : ContentView
    {
        public string LabelText { get; set; }

        public double LabelFontSize { get; set; }

        public string LabelFontFamily { get; set; }

        public LabelledCheckBox()
        {
            Content = new StackLayout
            {
                Children = {
                    new CheckBox(),
                    new Label {
                        Text = LabelText,
                        FontFamily = LabelFontFamily,
                        FontSize = LabelFontSize
                    }                    
                }
            };
        }
    }
}