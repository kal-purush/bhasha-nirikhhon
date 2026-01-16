using ICMXamarin.ViewModel;
using System;
using System.Collections.Generic;
using System.Text;
using Xamarin.Forms;

namespace ICMXamarin.View
{
    class FotoPage: ContentPage
    {
        FotoPageVM vm = new FotoPageVM();
        Button bFoto;
        public FotoPage()
        {
            BindingContext = vm;
            Title = "Fotos";
            Content = new StackLayout
            {
                Padding = 30,
                Children =
                {
                    bFoto
                }
            };
            bFoto = new Button
            {
                Text = "Tirar Foto",
                HorizontalOptions = LayoutOptions.FillAndExpand,
                VerticalOptions = LayoutOptions.FillAndExpand
            };
            bFoto.BindingContext = vm;
            bFoto.SetBinding(Button.TriggersProperty, "TakeFoto");

        }
    }
}