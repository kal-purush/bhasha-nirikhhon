using LagDaemon.YAMUD.ClientConsole.ConsoleIO;
using LagDaemon.YAMUD.ClientConsole.UserAccount;
using LagDaemon.YAMUD.Model.GameCommands;
using LagDaemon.YAMUD.Model.Scripting;
using LagDaemon.YAMUD.Model.User;
using LagDaemon.YAMUD.Model.Utilities;
using Microsoft.Extensions.DependencyInjection;
using Mono.Unix.Native;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;

internal class Application
{
    private ServiceProvider _serviceProvider;
    private bool _isRunning = true;
    GameContext _context;
    private HttpClient _mudHubClient;
    private AccountManager _accountManager;
    private ConsoleInputHandler _consoleHander = new ConsoleInputHandler();
    private bool _authenticated = false;
    private string _token = string.Empty;
    private Menu _topMenu;

    public Application(ServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider;
        _context = new GameContext() { CurrentUser = new LagDaemon.YAMUD.Model.User.UserAccount() };
        _mudHubClient = _serviceProvider.GetRequiredService<IHttpClientFactory>().CreateClient("MudHub");
        _accountManager = new AccountManager(_mudHubClient);
        _topMenu = new Menu("Select> ", new List<Menu.MenuItem>() { 
            new Menu.MenuItem() { 
                Entry = "Login to existing account", 
                Action = async () => { 
                    _authenticated = _accountManager.AuthenticateUser().Result;
                    if (_authenticated) await RunRepl();
                    else _isRunning = false;
                }
            },
            new Menu.MenuItem() { 
                Entry = "Create new account", 
                Action = async () => { 
                    _authenticated = _accountManager.CreateAccount().Result;
                    if (_authenticated) await RunRepl();
                    else _isRunning = false;
                } 
            },
            new Menu.MenuItem() { 
                Entry = "Exit", 
                Action = () => { 
                    _isRunning = false; 
                } 
            }
        });
    }

    public void Run()
    {

        Console.WriteLine("Starting application...");

        // Start REPL loop on a worker thread
        Task.Run(StartReplLoop);

        // You can add any other initialization logic here

        // Wait for user to exit
        while (_isRunning)
        {
            Thread.Sleep(1000);
        }
        Console.WriteLine("Application exiting");
    }

    private async Task StartReplLoop()
    {
        Console.WriteLine("REPL loop started.");

        while (_topMenu.Display()) { /* Just repeats */ }

    }

    private async Task RunRepl()
    {
        var commandParser = _serviceProvider.GetService<CommandParser>();
        _isRunning = _authenticated;

        while (_authenticated)
        {
            var input = _consoleHander.GetInput("yamud> ");

            // Parse and process the input
            var commands = commandParser.ParseCommandLine(input);
            foreach (var command in commands)
            {
                if (command.Type == CommandToken.Exit)
                {
                    Console.WriteLine("Exiting application...");
                    _isRunning = false;
                    return;
                }
                ProcessCommand(command);
            }
        }

    }

    private void ProcessCommand(Command command)
    {
        // Process the command based on its type and parameters
        Console.WriteLine($"Processing command: {command.Type}");
        foreach (var parameter in command.Parameters)
        {
            Console.WriteLine($"Parameter: {parameter}");
        }
        Console.WriteLine($"Execute: {command.Execute(_context)}");
    }

}


