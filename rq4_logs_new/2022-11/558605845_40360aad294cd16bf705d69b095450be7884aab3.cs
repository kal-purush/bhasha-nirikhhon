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
    /// Interaction logic for GameMenu.xaml
    /// </summary>
    public partial class GameMenu : Page, ServicesImplementation.IGameRoomManagementCallback
    {
        public ServicesImplementation.Users UserLogin { get; set; }
        public GameMenu()
        {
            InitializeComponent();
        }


        private void Button_NewGame_Click(object sender, RoutedEventArgs e)
        {
            InstanceContext context = new InstanceContext(this);
            ServicesImplementation.GameRoomManagementClient gameRoomClient = new ServicesImplementation.GameRoomManagementClient(context);
            int idNewRoom = gameRoomClient.CreateRoom(UserLogin);
            //Generate The nextPage
            GameRoom gameRoomPage = new GameRoom(idNewRoom,this.UserLogin);
            this.NavigationService.Navigate(gameRoomPage);
        }

        public void ReciveInvitationToRoom(int idRoom)
        {

        }

        public void UpdateRoom(Users[] usersInRoom)
        {

        }
    }
}