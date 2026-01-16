using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Android.App;
using Android.Content;
using Android.Graphics;
using Android.OS;
using Android.Runtime;
using Android.Views;
using Android.Widget;
using OnePiece.App.Droid.Effects;
using Xamarin.Forms;
using Xamarin.Forms.Platform.Android;

[assembly: ResolutionGroupName("CustomEffects")]
[assembly: ExportEffect(typeof(ColorSliderEffect), "ColorSliderEffect")]
namespace OnePiece.App.Droid.Effects
{
    public class ColorSliderEffect : PlatformEffect
    {
        protected override void OnAttached()
        {
            var seekBar = (SeekBar)Control;
            seekBar.ProgressDrawable.SetColorFilter(new PorterDuffColorFilter(Xamarin.Forms.Color.Red.ToAndroid(), PorterDuff.Mode.SrcIn));
            seekBar.Thumb.SetColorFilter(new PorterDuffColorFilter(Xamarin.Forms.Color.Red.ToAndroid(), PorterDuff.Mode.SrcIn));
        }

        protected override void OnDetached()
        {
        }
    }
}