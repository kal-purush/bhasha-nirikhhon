using System;
using Xamarin.Forms;
using Xamarin.Forms.Platform.iOS;
using UIKit;
using CoreGraphics;

namespace HomeAutomationApp.iOS
{

[assembly:ExportRenderer(typeof(HomeAutomationApp.HomeView), typeof(HomeAutomationApp.Droid.HomeViewRenderer))]
public class HomeViewRenderer : PageRenderer
{
	protected override void OnElementChanged (VisualElementChangedEventArgs e)
	{
		base.OnElementChanged (e);

		var page = e.NewElement as HomeView;
		var view = NativeView;

		var label = new UILabel (new CGRect (0, 40, 320, 40)) {
			Text = "Hello There"
		};

		view.Add (label);
	}
}
}
