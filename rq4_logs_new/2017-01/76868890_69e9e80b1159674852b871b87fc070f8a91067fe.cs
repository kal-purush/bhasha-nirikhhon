using OnePiece.App.Controls;
using OnePiece.App.Droid.Renderers;
using Xamarin.Forms;
using Xamarin.Forms.Platform.Android;

[assembly: ExportRenderer(typeof(LinkLabel), typeof(LinkLabelRenderer))]
namespace OnePiece.App.Droid.Renderers
{
    public class LinkLabelRenderer : LabelRenderer
    {
        protected override void OnElementChanged(ElementChangedEventArgs<Label> e)
        {
            base.OnElementChanged(e);

            var element = Element as LinkLabel;
            if (Control != null && element != null)
            {
                Control.Click += element.OnClicked;
            }
        }
    }
}