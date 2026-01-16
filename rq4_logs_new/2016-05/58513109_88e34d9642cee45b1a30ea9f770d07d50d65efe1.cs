using StaySteady.Mobile.Views;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace StaySteady.Mobile
{
	/*
	public class Exercise : TabbedPage
	{
		public Exercise ()
		{
			this.Children.Add (new Exercise1Xaml ());
			this.Children.Add (new Exercise2Xaml ());
			this.Children.Add (new Exercise3Xaml ());
		}
	}
	*/
	public class Exercise : CarouselPage
	{
		public Exercise ()
		{
			this.Children.Add (new Exercise1Xaml ());
			this.Children.Add (new Exercise2Xaml ());
			this.Children.Add (new Exercise3Xaml ());
		}
	}
}