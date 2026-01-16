using System;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace TopDownHeist.GameServer
{
    internal class GameLobby
    {
        private readonly ILogger<GameLobby> _logger;

        public string Name { get; set; }
        public string Password { get; set; }

        public GameLobby(ILogger<GameLobby> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        internal void Run()
        {
            Thread.Sleep(TimeSpan.FromSeconds(10));
        }
    }
}