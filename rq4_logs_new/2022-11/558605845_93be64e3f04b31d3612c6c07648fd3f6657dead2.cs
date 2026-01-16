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
using System.Windows.Shapes;
using System.ServiceModel;
using WPFLayer.ServicesImplementation;

namespace WPFLayer
{
    /// <summary>
    /// Interaction logic for AddFriendWindow.xaml
    /// </summary>
    public partial class AddFriendWindow : Window
    {
        public ServicesImplementation.Players PlayerLogin { set; get; }
        public List<ServicesImplementation.Players> PlayersList = new List<Players>();
        public AddFriendWindow(ServicesImplementation.Players player)
        {
            InitializeComponent();
            this.PlayerLogin = player;
        }

        public void RefreshPlayersListView()
        {
            this.ListView_Players.Items.Clear();
            foreach(ServicesImplementation.Players player in this.PlayersList)
            {
                ListViewItem playerDisplay = new ListViewItem();
                playerDisplay.Tag = player;
                playerDisplay.Content = player.playerName;
                this.ListView_Players.Items.Add(playerDisplay);
            }
        }

        private void Button_Search_Click(object sender, RoutedEventArgs e)
        {
            String filterWord = this.TextBox_SearchPlayer.Text;
            ServicesImplementation.PlayersManagementClient playersManagementClient = new ServicesImplementation.PlayersManagementClient();
            this.PlayersList.Clear();
            this.PlayersList.AddRange(playersManagementClient.GetFilteredPlayers(filterWord));
            this.RefreshPlayersListView();
        }

        private void Button_AddFriend_Click(object sender, RoutedEventArgs e)
        {
            ServicesImplementation.PlayersManagementClient playersManagementClient = new ServicesImplementation.PlayersManagementClient();
            ListViewItem itemSelected = (ListViewItem)this.ListView_Players.SelectedItem;
            if (itemSelected != null)
            {
                ServicesImplementation.Players playerTarget = (Players)itemSelected.Tag;
                if (!playersManagementClient.AddFriend(PlayerLogin, playerTarget))
                {
                    MessageBox.Show("No se puede añadir a ese jugador, ya se encuentra en su lista de amigos");
                }
                else
                {
                    MessageBox.Show("Se ha añadido el jugador" + playerTarget.playerName + " a su lista de amigos");
                }
            }else
            {
                MessageBox.Show("Seleccione Primero al Jugador que desea añadir como amigo");
            }

            }

        private void Button_Back_Click(object sender, RoutedEventArgs e)
        {
            this.Close();
        }
    }
}