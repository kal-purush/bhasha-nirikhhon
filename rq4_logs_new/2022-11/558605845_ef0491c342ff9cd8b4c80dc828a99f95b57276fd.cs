using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using WPFLayer.ServicesImplementation;

namespace WPFLayer
{
    /// <summary>
    /// Interaction logic for PodiumPage.xaml
    /// </summary>
    public partial class PodiumPage : Page
    {
        private List<GamesPlayers> players;
        public PodiumPage(List<GamesPlayers> players)
        {
            InitializeComponent();
            this.players = players;

            List<TextBlock> playerNames = new List<TextBlock>();
            playerNames.Add(TextBlock_PlayerName1);
            playerNames.Add(TextBlock_PlayerName2);
            playerNames.Add(TextBlock_PlayerName3);
            playerNames.Add(TextBlock_PlayerName4);

            List<TextBlock> playerScores = new List<TextBlock>();
            playerScores.Add(TextBlock_PlayerScore1);
            playerScores.Add(TextBlock_PlayerScore2);
            playerScores.Add(TextBlock_PlayerScore3);
            playerScores.Add(TextBlock_PlayerScore4);


            int playerCount = players.Count;
            for (int playerIndex = 0; playerIndex < playerCount; playerIndex++)
            { 
                playerNames[playerIndex].Text = players[playerIndex].Player.playerName;
                playerScores[playerIndex].Text = players[playerIndex].gameScore.ToString() ;
            }
        }

        private void Button_ReturnToMenu_Click(object sender, RoutedEventArgs e)
        {
            Console.WriteLine("Going BACK!!!");
            NavigationService.RemoveBackEntry();
            NavigationService.RemoveBackEntry();
            NavigationService.GoBack();
        }
    }
}