using Lego.EV3.Core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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

namespace Lego.EV3.HelloSensors
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        Brick _brick;
        int _forward = 40;
        int _backward = -30;
        uint _time = 300;



        public MainWindow()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            _brick = new Brick(new BluetoothCommunication("COM6"));
            _brick.BrickChanged += _brick_BrickChanged;
            _brick.ConnectAsync();
            _brick.DirectCommand.PlayToneAsync(10, 1000, 300);
        }

    private void _brick_BrickChanged(object sender, BrickChangedEventArgs e)
    {
        Debug.WriteLine("algo ocurrió");
        txtDistance.Content = e.Ports[InputPort.Four].SIValue.ToString();
    }


    private void ForwardButton_Click(object sender, RoutedEventArgs e)
    {
        _brick.DirectCommand.TurnMotorAtPowerForTimeAsync(OutputPort.B | OutputPort.C, _forward, _time, false);
    }


    private void RightButton_Click(object sender, RoutedEventArgs e)
    {
        _brick.DirectCommand.TurnMotorAtPowerForTimeAsync(OutputPort.B, _forward, _time, false);
    }

    private void LeftButton_Click(object sender, RoutedEventArgs e)
    {
        _brick.DirectCommand.TurnMotorAtPowerForTimeAsync(OutputPort.C, _forward, _time, false);
    }

    private void BackwardButton_Click(object sender, RoutedEventArgs e)
    {
        _brick.DirectCommand.TurnMotorAtPowerForTimeAsync(OutputPort.B | OutputPort.C, _backward, _time, false);
    }

       
    }
}