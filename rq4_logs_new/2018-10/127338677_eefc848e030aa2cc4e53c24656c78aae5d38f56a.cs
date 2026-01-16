// Based on the example from:
// https://github.com/aspnet/SignalR/blob/dev/samples/ClientSample/HubSample.cs
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Rocket.Player.Interfaces;

namespace Rocket.Player
{
    public class GamePlayer
    {
        private readonly ILoggerFactory _loggerFactory;
        private readonly ILogger<GamePlayer> _logger;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly Random _random = new Random();
        private readonly Stopwatch _stopwatch = new Stopwatch();
        private readonly GamePlayerOptions _options;
        private PlayerMetadata _playerMetadata;
        private Constant _constant;

        public GamePlayer(ILoggerFactory loggerFactory, GamePlayerOptions options)
        {
            _loggerFactory = loggerFactory;
            _logger = loggerFactory.CreateLogger<GamePlayer>();
            _options = options;

            Console.CancelKeyPress += (sender, a) =>
            {
                a.Cancel = true;
                Console.WriteLine("Closing down...");
                _cancellationTokenSource.Cancel();
            };
        }


        public async Task Play()
        {
            using (_logger.BeginScope(nameof(Play)))
            {
                _stopwatch.Start();
                var connection = new HubConnectionBuilder()
                    .WithUrl(_options.Url)
                    .AddMessagePackProtocol()
                    .Build();

                connection.On<Constant>("Constants", ConstantsUpdateHandler);
                connection.On<string, PlayerMetadata>("PlayerMetadataUpdate", PlayerMetadataUpdateUpdateHandler);
                connection.On<Interfaces.Player>("Update", PlayerUpdateHandler);
                connection.On<Shot>("Fire", ShotUpdateHandler);

                _logger.LogInformation(LoggingEvents.Connecting, $"Connecting to {_options.Url}...");
                await connection.StartAsync(_cancellationTokenSource.Token);
                _logger.LogInformation(LoggingEvents.Connected, "Connected!");

                while (_playerMetadata == null)
                {
                    _logger.LogInformation(LoggingEvents.WaitingMetadata, "Waiting for player metadata");
                    await Task.Delay(500, _cancellationTokenSource.Token);
                }

                if (_playerMetadata == null)
                {
                    _logger.LogError(LoggingEvents.WaitingMetadataCancelled, "Waiting for metadata has been cancelled.");
                    return;
                }

                var animationUpdate = _stopwatch.Elapsed.Ticks;
                _playerMetadata.Player.Time = animationUpdate / (double)TimeSpan.TicksPerMillisecond;

                var previouslySubmittedPlayer = new Interfaces.Player
                {
                    X = _playerMetadata.Player.X,
                    Y = _playerMetadata.Player.Y
                };

                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    var timestamp = _stopwatch.Elapsed.Ticks;
                    var delta = (timestamp - animationUpdate) / (double)TimeSpan.TicksPerSecond;

                    var player = _playerMetadata.Player;
                    if (player.Animation != null)
                    {
                        player.AnimationTiming -= delta * _constant.Animation.ExplosionAnimationDuration;
                        if (player.AnimationTiming <= 0)
                        {
                            player.Rotation = Math.PI * 3 / 2;
                            player.Speed = 0;
                            player.Animation = null;
                            player.AnimationTiming = 0;
                        }
                    }
                    else
                    {
                        var previousPlayer = new Interfaces.Player
                        {
                            Left = player.Left,
                            Right = player.Right,
                            Top = player.Top,
                            Fire1 = player.Fire1,
                            X = player.X,
                            Y = player.Y
                        };

                        UpdatePlayer(delta, player);
                        ProcessInput(player);

                        _logger.LogDebug($"Input state previousPlayer: Top: {previousPlayer.Top}, Left: {previousPlayer.Left}, Right: {previousPlayer.Right}, Fire1: {previousPlayer.Fire1}");
                        _logger.LogDebug($"Input state player:         Top: {player.Top}, Left: {player.Left}, Right: {player.Right}, Fire1: {player.Fire1}");
                        _logger.LogDebug($"Input changes:              Top: {previousPlayer.Top != player.Top}, Left: {previousPlayer.Left != player.Left}, Right: {previousPlayer.Right != player.Right}, Fire1: {previousPlayer.Fire1 != player.Fire1}");

                        var deltaX = previousPlayer.X - player.X;
                        var deltaY = previousPlayer.Y - player.Y;
                        var distance = Math.Sqrt(deltaX * deltaX + deltaY * deltaY);
                        _logger.LogDebug($"distance change: {distance}");

                        var deltaSubmitX = previouslySubmittedPlayer.X - player.X;
                        var deltaSubmitY = previouslySubmittedPlayer.Y - player.Y;
                        var deltaSubmitDistance = Math.Sqrt(deltaSubmitX * deltaSubmitX + deltaSubmitY * deltaSubmitY);
                        _logger.LogDebug($"previously submitted distance change: {deltaSubmitDistance}");

                        if (previousPlayer.Left != player.Left ||
                            previousPlayer.Right != player.Right ||
                            previousPlayer.Top != player.Top ||
                            previousPlayer.Fire1 != player.Fire1 ||
                            deltaSubmitDistance > 50 ||
                            (timestamp - previouslySubmittedPlayer.Time) / (double)TimeSpan.TicksPerMillisecond > _constant.Network.SendUpdateFrequency)
                        {
                            _logger.LogInformation($"Send Time: {player.Time} (delta: {(timestamp - previouslySubmittedPlayer.Time) / (double)TimeSpan.TicksPerMillisecond})");
                            player.Time = timestamp / (double)TimeSpan.TicksPerMillisecond;

                            await connection.InvokeAsync(
                                "Update",
                                player,
                                _cancellationTokenSource.Token);

                            previouslySubmittedPlayer.X = player.X;
                            previouslySubmittedPlayer.Y = player.Y;
                            previouslySubmittedPlayer.Time = timestamp;
                        }
                    }

                    animationUpdate = timestamp;
                    await Task.Delay(25, _cancellationTokenSource.Token);
                }
            }
        }

        private void UpdatePlayer(double delta, Interfaces.Player player)
        {
            using (_logger.BeginScope(nameof(UpdatePlayer)))
            {
                _logger.LogDebug($"Before player.X: {player.X}");
                _logger.LogDebug($"Before player.Y: {player.Y}");
                _logger.LogDebug($"Before player.Speed: {player.Speed}");
                _logger.LogDebug($"Before player.Rotation: {player.Rotation}");

                if (player.Left)
                {
                    player.Rotation -= delta * _constant.Movement.TurnRate;
                    if (player.Rotation < 0)
                    {
                        player.Rotation += 2 * Math.PI;
                    }
                }
                if (player.Right)
                {
                    player.Rotation += delta * _constant.Movement.TurnRate;
                    if (player.Rotation > 2 * Math.PI)
                    {
                        player.Rotation -= 2 * Math.PI;
                    }
                }
                if (player.Top)
                {
                    player.Speed += delta * _constant.Movement.AccelerationRate;
                    if (player.Speed > _constant.Movement.MaxSpeedPerSecond)
                    {
                        player.Speed = _constant.Movement.MaxSpeedPerSecond;
                    }
                }
                else if (player.Speed > 0)
                {
                    player.Speed -= delta * _constant.Movement.DeAccelerationRate;
                    if (player.Speed < 1)
                    {
                        player.Speed = 0;
                    }
                }
                if (player.Bottom)
                {
                    player.Speed -= delta * _constant.Movement.BrakeRate;
                    if (player.Speed < 0)
                    {
                        player.Speed = 0;
                    }
                }

                var deltaX = delta * player.Speed * Math.Cos(player.Rotation);
                var deltaY = delta * player.Speed * Math.Sin(player.Rotation);

                _logger.LogDebug($"delta:  {delta}");
                _logger.LogDebug($"deltaX: {deltaX}");
                _logger.LogDebug($"deltaY: {deltaY}");

                player.X += deltaX;
                player.Y += deltaY;

                _logger.LogDebug($"After player.X: {player.X}");
                _logger.LogDebug($"After player.Y: {player.Y}");
                _logger.LogDebug($"After player.Speed: {player.Speed}");
                _logger.LogDebug($"After player.Rotation: {player.Rotation}");
            }
        }

        private void ProcessInput(Interfaces.Player player)
        {
            using (_logger.BeginScope(nameof(ProcessInput)))
            {
                player.Fire1 = false; // _random.NextDouble() > 0.9;
                player.Top = true; // _random.NextDouble() > 0.1;
                if (player.Top)
                {
                    player.Left = player.Left ? _random.NextDouble() > 0.02 : _random.NextDouble() > 0.98;
                    player.Right = !player.Left;
                }
                else
                {
                    player.Left = player.Right = false;
                }
            }
        }

        private void ConstantsUpdateHandler(Constant constant)
        {
            using (_logger.BeginScope(nameof(ConstantsUpdateHandler)))
            {
                _logger.LogDebug(LoggingEvents.ConstantsReceived, $"Constants received");
                _constant = constant;
            }
        }

        private void ShotUpdateHandler(Shot shot)
        {
            using (_logger.BeginScope(nameof(ShotUpdateHandler)))
            {
                _logger.LogDebug(LoggingEvents.ShotReceived, $"Shot received {shot.ID}");
            }
        }

        private void PlayerUpdateHandler(Interfaces.Player player)
        {
            using (_logger.BeginScope(nameof(PlayerUpdateHandler)))
            {
                _logger.LogDebug(LoggingEvents.PlayerReceived, $"Player received {player.ID}");
            }
        }

        private void PlayerMetadataUpdateUpdateHandler(string action, PlayerMetadata playerMetadata)
        {
            using (_logger.BeginScope(nameof(PlayerMetadataUpdateUpdateHandler)))
            {
                _logger.LogDebug(LoggingEvents.PlayerMetadataReceived, $"Player update: {action} {playerMetadata.ID}");
                if (action == PlayerActions.Add)
                {
                    //this.others.set(playerMetadata.id, playerMetadata);
                }
                else if (action == PlayerActions.Delete)
                {
                    //this.others.delete(playerMetadata.id);
                }
                else if (action == PlayerActions.Self)
                {
                    _playerMetadata = playerMetadata;
                }
            }
        }
    }
}