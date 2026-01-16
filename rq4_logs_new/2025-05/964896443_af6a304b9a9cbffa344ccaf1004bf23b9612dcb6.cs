using System;
using Smartitecture.API;
using Smartitecture.Services;
using Smartitecture.ViewModels;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.UI.Xaml;
using Windows.ApplicationModel;

namespace Smartitecture
{
    /// <summary>
    /// Main application class that provides application-specific behavior to supplement the default Application class.
    /// Handles initialization of services, API hosting, and UI components.
    /// </summary>
    public partial class App : Application
    {
        private Window _window;
        private IServiceProvider _serviceProvider;
        private ApiHostService _apiHostService;

        /// <summary>
        /// Initializes a new instance of the App class.
        /// </summary>
        public App()
        {
            this.InitializeComponent();
            this.Suspending += OnSuspending;
        }

        /// <summary>
        /// Gets the service provider for the application.
        /// </summary>
        public IServiceProvider Services => _serviceProvider;

        /// <summary>
        /// Gets the current application instance.
        /// </summary>
        public new static App Current => (App)Application.Current;

        /// <summary>
        /// Invoked when the application is launched.
        /// Sets up dependency injection, initializes services, and starts the API host.
        /// </summary>
        /// <param name="args">Details about the launch request and process.</param>
        protected override async void OnLaunched(Microsoft.UI.Xaml.LaunchActivatedEventArgs args)
        {
            // Set up configuration
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            // Set up dependency injection
            var services = new ServiceCollection();

            // Register logging
            services.AddLogging(builder =>
            {
                builder.AddDebug();
                builder.AddConsole();
            });

            // Register configuration
            services.Configure<AzureOpenAIConfiguration>(
                configuration.GetSection("AzureOpenAI"));

            // Register core services
            services.AddSingleton<ILLMService, AzureOpenAIService>();
            services.AddSingleton<CommandMapper>();

            // Register agent services
            services.AddAgentServices(configuration);

            // Register ViewModels
            services.AddTransient<AgentViewModel>();
            services.AddTransient<SecurityViewModel>();
            services.AddTransient<ScreenAnalysisViewModel>();
            
            // Register theme service
            services.AddSingleton<ThemeService>();
            
            // Register language service
            services.AddSingleton<LanguageService>();

            _serviceProvider = services.BuildServiceProvider();

            // Initialize agent system
            ServiceCollectionExtensions.InitializeAgentSystem(_serviceProvider);

            // Initialize theme service
            var themeService = _serviceProvider.GetRequiredService<ThemeService>();
            await themeService.InitializeThemeAsync();
            
            // Initialize language service
            var languageService = _serviceProvider.GetRequiredService<LanguageService>();
            await languageService.InitializeLanguageAsync();

            // Initialize API host
            var llmService = _serviceProvider.GetRequiredService<ILLMService>();
            var commandMapper = _serviceProvider.GetRequiredService<CommandMapper>();
            
            _apiHostService = new ApiHostService(llmService, commandMapper);
            await _apiHostService.StartAsync();

            // Create and activate the main window
            _window = new MainWindow();
            _window.Activate();
        }

        /// <summary>
        /// Invoked when application execution is being suspended.
        /// Saves application state and stops any background activity.
        /// </summary>
        /// <param name="sender">The source of the suspend request.</param>
        /// <param name="e">Details about the suspend request.</param>
        private async void OnSuspending(object sender, SuspendingEventArgs e)
        {
            var deferral = e.SuspendingOperation.GetDeferral();
            
            // Stop the API host service
            if (_apiHostService != null)
            {
                await _apiHostService.StopAsync();
            }
            
            deferral.Complete();
        }
    }
}