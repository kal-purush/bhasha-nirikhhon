using Snake.Classes;
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

namespace Snake.Pages.Game
{
    public partial class GameOverPage : Page
    {
        private readonly GameMenu gameMenu;

        public GameOverPage(GameMenu gameMenu)
        {
            InitializeComponent();
            this.gameMenu = gameMenu;
        }
    }
}