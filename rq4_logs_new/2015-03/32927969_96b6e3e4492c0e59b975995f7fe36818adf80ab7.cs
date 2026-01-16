using System;


using WebKaldTest;
using Xamarin.Forms;

namespace WebKaldTest
{
	public class Menu : ContentPage
	{
		Label l1;
		Button b1;
		Button b2;
		Button b3;

		public Menu ()
		{
		    l1 = new Label {
				Text = "Menu",
				Font = Font.SystemFontOfSize(50, FontAttributes.Bold),
				HorizontalOptions = LayoutOptions.Center
			};

			b1 = new Button {
				Text = "Sagsliste",				
				BackgroundColor = Color.Silver,
				TextColor = Color.Black,
				HorizontalOptions = LayoutOptions.Fill,
				VerticalOptions = LayoutOptions.Center
			};

			b2 = new Button {
				Text = "Datafangst",
				BackgroundColor = Color.Silver,
				TextColor = Color.Black,
				HorizontalOptions = LayoutOptions.Fill,
				VerticalOptions = LayoutOptions.Center
			};

			b3 = new Button {
				Text = "Tid",
				BackgroundColor = Color.Silver,
				TextColor = Color.Black,
				HorizontalOptions = LayoutOptions.Fill,
				VerticalOptions = LayoutOptions.Center
			};
			this.Content = new StackLayout {

				Children = {
					l1,
					b1,
					b2,
					b3
				}
			};
			
		}
	}
}

