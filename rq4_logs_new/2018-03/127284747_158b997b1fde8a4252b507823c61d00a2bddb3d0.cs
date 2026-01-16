using Foundation;
using System.ComponentModel;
using UIKit;
using Xamarin.Forms;
using Xamarin.Forms.Platform.iOS;
using XamCustomPlaceholderFont.Controls;
using XamCustomPlaceholderFont.iOS.CustomRenderers;

[assembly: ExportRenderer(typeof(CustomPlaceholderEntry), typeof(CustomPlaceholderEntryRenderer))]
namespace XamCustomPlaceholderFont.iOS.CustomRenderers
{
    public class CustomPlaceholderEntryRenderer : EntryRenderer
    {
        private CustomPlaceholderEntry CustomElement => Element as CustomPlaceholderEntry;
        protected override void OnElementChanged(ElementChangedEventArgs<Entry> e)
        {
            base.OnElementChanged(e);

            UpdatePlaceholderFont();
        }

        protected override void OnElementPropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            base.OnElementPropertyChanged(sender, e);

            if (e.PropertyName == CustomPlaceholderEntry.PlaceholderFontProperty.PropertyName)
                UpdatePlaceholderFont();
        }

        private void UpdatePlaceholderFont()
        {
            if (CustomElement.Placeholder != null)
            {
                var paragraphStyle = new NSMutableParagraphStyle
                {
                    Alignment = UITextAlignment.Left
                };

                var placeholderFont = CustomElement.PlaceholderFont.ToUIFont();

                Control.AttributedPlaceholder = new NSAttributedString(CustomElement.Placeholder, placeholderFont, paragraphStyle: paragraphStyle);
            }
        }
    }
}