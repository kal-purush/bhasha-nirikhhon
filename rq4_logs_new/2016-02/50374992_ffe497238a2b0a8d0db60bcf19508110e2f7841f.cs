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
using Lego.EV3.Core;
using System.Diagnostics;

namespace Lego.EV3.HelloMotors
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        Brick _brick;
        
        public MainWindow()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            _brick = new Brick(new BluetoothCommunication("COM6"));
            _brick.BrickChanged += _brick_BrickChanged;
            _brick.ConnectAsync();
            _brick.DirectCommand.PlayToneAsync(100, 100, 1000);
        }

        private void _brick_BrickChanged(object sender, BrickChangedEventArgs e)
        {
            Debug.WriteLine("algo ocurrió");

        }
    }
}