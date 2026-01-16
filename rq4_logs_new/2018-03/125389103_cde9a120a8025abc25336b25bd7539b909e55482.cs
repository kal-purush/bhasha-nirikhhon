using Android.Content;
using Android.Graphics.Drawables;
using PhoneDirectory.Core.CustomRenderers;
using PhoneDirectory.Droid.CustomRenderers;
using Xamarin.Forms;
using Xamarin.Forms.Platform.Android;

[assembly: ExportRenderer(typeof(RoundedButton), typeof(RoundedButtonRenderer))]
namespace PhoneDirectory.Droid.CustomRenderers
{
    public class RoundedButtonRenderer : ButtonRenderer
    {
        public RoundedButtonRenderer(Context context) : base(context)
        {
        }

		protected override void OnElementChanged(ElementChangedEventArgs<Button> e)
		{
            base.OnElementChanged(e);

            if (Control != null)
            {
                GradientDrawable gradientDrawable = new GradientDrawable();
                gradientDrawable.SetShape(ShapeType.Rectangle);
                gradientDrawable.SetColor(Element.BackgroundColor.ToAndroid());
                gradientDrawable.SetStroke(4, Element.BorderColor.ToAndroid());
                gradientDrawable.SetCornerRadius(40.0f);

                Control.SetBackground(gradientDrawable);
            }    
		}
	}
}