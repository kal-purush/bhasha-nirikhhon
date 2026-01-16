using LagDaemon.YAMUD.ClientConsole.UserAccount;
using LagDaemon.YAMUD.Model.GameCommands;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Configuration;

namespace LagDaemon.YAMUD.ClientConsole.Services
{
    public class CommandService
    {
        public event Action<Command> CommandReceived;

        private string? _commandHubUrl;
        private AccountManager _accountManager;
        private HubConnection _connection;

        public CommandService(IConfiguration config, AccountManager accountManager)
        {
            _commandHubUrl = config["CommandHubUrl"];
            _accountManager = accountManager;
        }

        public async Task ConnectAsync()
        {
            //_connection = new HubConnectionBuilder()
            //      .WithUrl(_commandHubUrl, options => {
            //          options.AccessTokenProvider = () => Task.FromResult(_accountManager.Token);
            //      }).Build();

            //_connection.On<Command>("ReceiveCommand", command =>
            //{
            //    CommandReceived?.Invoke(command);
            //});


        }

    }
}