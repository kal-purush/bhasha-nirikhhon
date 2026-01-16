using chinese_checkers.Core.Models;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Foundation;
using Windows.Foundation.Collections;
using Windows.UI.Xaml;
using Windows.UI.Xaml.Controls;
using Windows.UI.Xaml.Controls.Primitives;
using Windows.UI.Xaml.Data;
using Windows.UI.Xaml.Input;
using Windows.UI.Xaml.Media;
using Windows.UI.Xaml.Media.Imaging;
using Windows.UI.Xaml.Navigation;

// The Content Dialog item template is documented at https://go.microsoft.com/fwlink/?LinkId=234238

namespace chinese_checkers.Views.Menu.Dialogs
{
    public sealed partial class GameEndedDialog : ContentDialog
    {
        public ScoreBoard ScoreBoard { get; set; }
        public GameEndedDialog(ScoreBoard scoreBoard)
        {
            this.InitializeComponent();
            ScoreBoard = scoreBoard;
        }

        private void ContentDialog_PrimaryButtonClick(ContentDialog sender, ContentDialogButtonClickEventArgs args)
        {
            (Window.Current.Content as Frame)?.Navigate(typeof(MainMenu), null);
        }

        private void testgrid_Loaded(object sender, RoutedEventArgs e)
        {
            var ordered = ScoreBoard.ScoreBoardEntries.OrderBy(x => x.Player.Placement).ToList();
            var colDef = new ColumnDefinition();
            colDef.Width = GridLength.Auto;
            testgrid.ColumnDefinitions.Add(colDef);

            foreach (var item in ordered)
            {
                var rowDef = new RowDefinition();
                
                testgrid.RowDefinitions.Add(rowDef);
                
                Image img = new Image();
                img.Width = 50.0;
                img.Height = 50.0;
                switch (item.Player.Placement)
                {
                    case 1:
                        img.Source = new BitmapImage(new Uri("ms-appx:///Assets/Images/Medals/medal_04_gold.png"));
                        break;
                    case 2:
                        img.Source = new BitmapImage(new Uri("ms-appx:///Assets/Images/Medals/medal_04_silver.png"));
                        break;
                    case 3:
                        img.Source = new BitmapImage(new Uri("ms-appx:///Assets/Images/Medals/medal_04_bronze.png"));
                        break;
                    default:
                        break;
                }
                
                

                TextBlock txt = new TextBlock();

                testgrid.Children.Add(img);
                testgrid.Children.Add(txt);
                

                Grid.SetRow(txt, ordered.IndexOf(item));
                Grid.SetRow(img, ordered.IndexOf(item));

                Grid.SetColumn(img, 0);
                Grid.SetColumn(txt, 1);
                

                txt.Text = item.Player.ToString();
                

            }
        }
    }
}