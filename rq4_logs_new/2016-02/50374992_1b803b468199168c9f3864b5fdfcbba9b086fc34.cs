using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lego.EV3.Core;
using System.Threading;
using System.Configuration;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Lego.EV3.Shared;
using System.IO;
using System.Runtime.Serialization.Json;
using Newtonsoft.Json;

namespace Lego.EV3.ConsoleVote
{
    class Program
    {
        static Brick _brick;
        static void Main(string[] args)
        {
            StartLego();
            Start();
        }

        private static async void StartLego()
        {
            _brick = new Brick(new BluetoothCommunication("COM6"));
            _brick.BrickChanged += _brick_BrickChanged;

            Console.WriteLine("Connecting...");
            await _brick.ConnectAsync();
            await _brick.DirectCommand.PlayToneAsync(100, 40000, 300);

            System.Console.WriteLine("Connected...");
        }

        static void _brick_BrickChanged(object sender, BrickChangedEventArgs e)
        {
            System.Console.WriteLine(e.Ports[InputPort.Four].SIValue);
        }

        static void Start()
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

        static public async void ReceiveMessage(string connectionString, string topicPath, string subscriptionName, CancellationToken token)
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
                        Console.WriteLine("Movimiento: "+ movement.Movement +"- Votos: "+ movement.Votes);

                        MoveLego(movement.Movement);
                        await brokeredMessage.CompleteAsync();

                    }
                }
                catch (Exception ex)
                {

                    System.Diagnostics.Trace.WriteLine($"{ex.Message}");
                }
            }

        }

        private static async void MoveLego(DriveCommand movement)
        {
            switch (movement)
            {
                case DriveCommand.Forward:

                    ////mueve la rueda izquierda
                    await _brick.DirectCommand.TurnMotorAtSpeedForTimeAsync(OutputPort.B, 0x25, 1000, false);
                    ////mueve la rueda derecha
                    await _brick.DirectCommand.TurnMotorAtSpeedForTimeAsync(OutputPort.C, 0x25, 1000, false);

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
                    await _brick.DirectCommand.TurnMotorAtSpeedForTimeAsync(OutputPort.B, -0x25, 1000, false);
                    ////mueve la rueda derecha
                    await _brick.DirectCommand.TurnMotorAtSpeedForTimeAsync(OutputPort.C, -0x25, 1000, false);
                    break;
            }
        }
    }
}