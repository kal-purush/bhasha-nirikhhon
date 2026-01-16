using BrainyStories.Objects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace BrainyStories {
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class RewardsPage : ContentPage {
        public RewardsPage()
        {
            InitializeComponent();
            User user = new User();
            int numOfBronzeCoins = user.rewardsRecieved["Bronze"];
            int numOfSilverCoins = user.rewardsRecieved["Silver"] + (numOfBronzeCoins / 5);
            int numOfGoldCoins = user.rewardsRecieved["Gold"] + (numOfSilverCoins / 5);
            int numOfBags = numOfGoldCoins / 5;
            numOfBronzeCoins = numOfBronzeCoins % 5;
            numOfSilverCoins = numOfSilverCoins % 5;
            numOfGoldCoins = numOfGoldCoins % 5;
            for (int i = 0; i < numOfBags; i++)
            {
                Image bag = new Image() { Source = "General.png" };
                GoldList.Children.Add(bag);
            }
            for (int i = 0; i < numOfGoldCoins; i++)
            {
                Image goldCoin = new Image() { Source = "Animal.png" };
                GoldList.Children.Add(goldCoin);
            }

            for (int i = 0; i < numOfSilverCoins; i++)
            {
                Image silverCoin = new Image() { Source = "Male.png" };
                SilverList.Children.Add(silverCoin);
            }

            for (int i = 0; i < numOfBronzeCoins; i++)
            {
                Image bronzeCoin = new Image() { Source = "Female.png" };
                BronzeList.Children.Add(bronzeCoin);
            }
        }
    }
}