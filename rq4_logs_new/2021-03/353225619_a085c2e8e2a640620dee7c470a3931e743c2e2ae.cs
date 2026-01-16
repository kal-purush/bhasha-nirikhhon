using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Practica7.Models;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace Practica7.Views
{
	[XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class EntryPage : ContentPage
	{
		public EntryPage()
		{
			Content = new StackLayout
			{
				Children = {
					new Label {
						Text = "Hello Wordl! Estoy haciendo un Custom Renderer",
					},
					new MyEntry {
						Text = "Cambiando...",
					}
				},
				VerticalOptions = LayoutOptions.CenterAndExpand,
				HorizontalOptions = LayoutOptions.CenterAndExpand,
			};
		}

        
    }
}