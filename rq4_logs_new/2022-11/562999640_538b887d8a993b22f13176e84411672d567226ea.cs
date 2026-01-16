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

namespace Trilha.Views
{
    /// <summary>
    /// Lógica interna para GameTable.xaml
    /// </summary>
    public partial class GameTable : Window
    {
        public GameTable()
        {
            InitializeComponent();
        }


        /// <summary>
        /// Método responsável por deixar com que o usuário movimente a janela livremente com o botão esquerdo do mouse
        /// </summary>
        private void MoveWindow(object sender, MouseButtonEventArgs e)
        {
            if (e.ChangedButton == MouseButton.Left)
                this.DragMove();
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            Peca1.Background = Brushes.Red;
        }
    }
}