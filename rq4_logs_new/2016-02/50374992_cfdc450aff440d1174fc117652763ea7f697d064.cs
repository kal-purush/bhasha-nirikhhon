using System.Windows;
using System.IO.Ports;
using Lego.Ev3.Core;
using Lego.Ev3.Desktop;
using Lego.EV3.Shared;
using System.Threading;
using System.Configuration;
using Microsoft.ServiceBus;
using System;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System.Windows.Controls;
using System.Collections.Generic;
using System.Collections;

namespace Lego.EV3.WPFVote
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        Brick _brick;
        static DateTime lastime = TimeZoneInfo.ConvertTimeToUtc(DateTime.Now);

        public MainWindow()
        {
            InitializeComponent();
        }

        private void Window_Loaded(object sender, RoutedEventArgs e)
        {
            Ports.ItemsSource = SerialPort.GetPortNames();
            Ports.SelectedIndex = (Ports.ItemsSource as string[]).Length - 1;            
        }

        private void Connect_Click(object sender, RoutedEventArgs e)
        {
            _brick = new Brick(new BluetoothCommunication((string)Ports.SelectedItem));
            _brick.BrickChanged += brick_BrickChanged;
            _brick.ConnectAsync();
            _brick.DirectCommand.PlayToneAsync(10, 1000, 300);
            connected.Content = "¡¡Conectao'!!";
            connected.Foreground = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Colors.Green);

            Start();
        }

        private void brick_BrickChanged(object sender, BrickChangedEventArgs e)
        {
            txtDistance.Content = e.Ports[InputPort.Four].SIValue.ToString();
        }

        void Start()
        {

            try
            {
                CancellationTokenSource source = new CancellationTokenSource();
                string connectionString = ConfigurationManager.AppSettings["TopicConnectionString"];

                var namespaceManager = NamespaceManager.CreateFromConnectionString(connectionString);


                if (!namespaceManager.TopicExists("legotopic"))
                {
                    namespaceManager.CreateTopic("legotopic");
                }

                ReceiveMessage(connectionString, "legotopic", "legosubs", source.Token);

                Console.ReadKey();
                source.Cancel();
                Console.ReadKey();
            }
            catch (Exception ex)
            {

                System.Diagnostics.Trace.WriteLine($"{ex.Message}");
            }

        }

        public async void ReceiveMessage(string connectionString, string topicPath, string subscriptionName, CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    var subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString, topicPath, subscriptionName);
                    var brokeredMessage = await subscriptionClient.ReceiveAsync(TimeSpan.FromSeconds(1));


                    if (brokeredMessage != null)
                    {
                        var message = brokeredMessage.GetBody<string>();

                        var movement = JsonConvert.DeserializeObject<EventHubVotes>(message);


                        if (!lastime.Equals(movement.Time))
                        {
                            Console.WriteLine("Movimiento: " + movement.Movement + " - Votos: " + movement.Votes + " - Time: " + movement.Time);

                            MoveLego(movement.Movement);
                            WriteMovement(movement.Movement);
                            lastime = movement.Time;
                            await brokeredMessage.CompleteAsync();

                        }


                    }
                }
                catch (Exception ex)
                {

                    System.Diagnostics.Trace.WriteLine($"{ex.Message}");
                }
            }

        }

        private void WriteMovement(DriveCommand movement) {
            deleteButtons();
            string name = movement.ToString();
            var boton = (Button)arrows.FindName(name);
            boton.Visibility = Visibility.Visible;       
        }

        private async void MoveLego(DriveCommand movement)
        {
            switch (movement)
            {
                case DriveCommand.Forward:
                    ////mueve la rueda izquierda y mueve la rueda derecha
                    await _brick.DirectCommand.TurnMotorAtSpeedForTimeAsync(OutputPort.C| OutputPort.B, 0x25, 1000, false);

                    break;
                case DriveCommand.Right:
                    ////mueve la rueda izquierda
                    await _brick.DirectCommand.TurnMotorAtSpeedForTimeAsync(OutputPort.B, 0x25, 1000, false);

                    break;
                case DriveCommand.Left:
                    ////mueve la rueda derecha
                    await _brick.DirectCommand.TurnMotorAtSpeedForTimeAsync(OutputPort.C, 0x25, 1000, false);

                    break;
                case DriveCommand.Back:
                    ////mueve la rueda derecha y la izquierda en velocidad negativa
                    await _brick.DirectCommand.TurnMotorAtSpeedForTimeAsync(OutputPort.C | OutputPort.B, -0x25, 1000, false);
                    break;
            }
        }

        public static List<T> GetLogicalChildCollection<T>(object parent) where T : DependencyObject
        {
            List<T> logicalCollection = new List<T>();
            GetLogicalChildCollection(parent as DependencyObject, logicalCollection);
            return logicalCollection;
        }

        private static void GetLogicalChildCollection<T>(DependencyObject parent, List<T> logicalCollection) where T : DependencyObject
        {
            IEnumerable children = LogicalTreeHelper.GetChildren(parent);
            foreach (object child in children)
            {
                if (child is DependencyObject)
                {
                    DependencyObject depChild = child as DependencyObject;
                    if (child is T)
                    {
                        logicalCollection.Add(child as T);
                    }
                    GetLogicalChildCollection(depChild, logicalCollection);
                }
            }
        }

        private void deleteButtons() {
            List<Button> buttons = GetLogicalChildCollection<Button>(arrows);
            foreach (Button button in buttons) {
                button.Visibility = Visibility.Hidden;
            }
        }


    }
}