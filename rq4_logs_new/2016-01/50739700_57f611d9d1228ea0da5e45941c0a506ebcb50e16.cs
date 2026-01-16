using System;

using Xamarin.Forms;

namespace LifeStyle
{
	public class Cadastra_atividade : ContentPage
	{
		public Cadastra_atividade(){

		Title = "Cadastrar Treino";
		Icon = "icon.png";
		Content = new StackLayout {
			Spacing = 20, Padding = 40,
			VerticalOptions = LayoutOptions.Center,
			Children = {

				new Entry { Placeholder =  "Descrição"},

				new Button {
					Text = "Avançar",
					TextColor = Color.White,
					BackgroundColor = Color.FromHex("77D065") 
				}}
		};
	}
		}
	}


