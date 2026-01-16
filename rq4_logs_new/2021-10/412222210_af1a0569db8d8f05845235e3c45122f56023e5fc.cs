namespace RedPandaBot
{
    public class RedPandaBot
    {
        public DiscordShardedClient? ShardedClient { get; private set; }
        public InteractivityExtension? Interactivity { get; private set; }
        public SlashCommandsExtension? SlashCommands { get; private set; }
        public static ConfigJson Config = new ConfigJson();
        public static readonly EventId BotEventId = new EventId(1);

        public static void Main(string[] args)
        {
            var bot = new RedPandaBot();
            try
            {
                bot.RunAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Log.Logger.Fatal($"{ex.Message} | {ex.StackTrace}");
            }
        }

        public async Task RunAsync()
        {
            // Grab ConfigJson and deserialize - helps to keep token hidden.
            string json = await File.ReadAllTextAsync("Configuration/configuration.json").ConfigureAwait(false);
            using (var fs = File.OpenRead("Configuration/configuration.json")) Config = JsonConvert.DeserializeObject<ConfigJson>(json);

            var configJson = JsonConvert.DeserializeObject<ConfigJson>(json);

            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console(theme: SystemConsoleTheme.Literate, restrictedToMinimumLevel: LogEventLevel.Information)
                .WriteTo.File("log.txt",
                    outputTemplate: "{Timestamp:dd MMM yyyy - hh:mm:ss tt} [{Level:u3}] {Message:lj}{NewLine}{Exception}")
                .MinimumLevel.Debug()
                .CreateLogger();

            var logFactory = new LoggerFactory().AddSerilog();

            ShardedClient = new DiscordShardedClient(new DiscordConfiguration()
            {
                Token = configJson.Token,
                TokenType = TokenType.Bot,
                AutoReconnect = true,
                LoggerFactory = logFactory,
                Intents = DiscordIntents.All
            });

            await ShardedClient.UseInteractivityAsync(new InteractivityConfiguration()
            {
                Timeout = TimeSpan.FromSeconds(60),
                PollBehaviour = PollBehaviour.KeepEmojis,
                PaginationBehaviour = PaginationBehaviour.WrapAround,
                ButtonBehavior = ButtonPaginationBehavior.Disable,
                ResponseBehavior = InteractionResponseBehavior.Respond,
                ResponseMessage = new string($"Invalid response."),
                AckPaginationButtons = true,
                PaginationButtons = new PaginationButtons()
                {
                    SkipLeft = new DiscordButtonComponent(ButtonStyle.Primary, "first", "First"),
                    Left = new DiscordButtonComponent(ButtonStyle.Success, "left", "Left"),
                    Stop = new DiscordButtonComponent(ButtonStyle.Danger, "stop", "Stop"),
                    Right = new DiscordButtonComponent(ButtonStyle.Success, "right", "Right"),
                    SkipRight = new DiscordButtonComponent(ButtonStyle.Primary, "last", "Last"),
                }
            });

            var slashCommandsConfig = await ShardedClient.UseSlashCommandsAsync();

            // Command registration will go here

            // Client, Slash Command and error event binding will happen here

            Log.Information("Connecting to Discord..");
            await ShardedClient.StartAsync().ConfigureAwait(false);
            Log.Information("Connected to Discord successfully!");

            Log.Information($"Bot version {Assembly.GetExecutingAssembly().GetName().Version}");

            await Task.Delay(-1).ConfigureAwait(false);
        }
    }
}