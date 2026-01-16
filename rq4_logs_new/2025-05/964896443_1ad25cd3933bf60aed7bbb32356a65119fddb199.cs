using System;
using System.Threading;
using System.Threading.Tasks;
using AIPal.API.Services;
using AIPal.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace AIPal.API
{
    /// <summary>
    /// Service responsible for hosting the API within the WinUI desktop application.
    /// Manages the lifecycle of the web host that serves the API endpoints.
    /// </summary>
    public class ApiHostService : IDisposable
    {
        private IHost _host;
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private readonly ILLMService _llmService;
        private readonly CommandMapper _commandMapper;
        private readonly int _port;
        private bool _isDisposed;

        /// <summary>
        /// Initializes a new instance of the ApiHostService.
        /// </summary>
        /// <param name="llmService">The language model service to be used by the API</param>
        /// <param name="commandMapper">The command mapper to be used by the API</param>
        /// <param name="port">The port on which to host the API (default: 5000)</param>
        public ApiHostService(ILLMService llmService, CommandMapper commandMapper, int port = 5000)
        {
            _llmService = llmService ?? throw new ArgumentNullException(nameof(llmService));
            _commandMapper = commandMapper ?? throw new ArgumentNullException(nameof(commandMapper));
            _port = port;
        }

        /// <summary>
        /// Starts the API host service.
        /// </summary>
        /// <returns>A task representing the asynchronous operation</returns>
        public async Task StartAsync()
        {
            _host = CreateHostBuilder().Build();
            await _host.StartAsync(_cancellationTokenSource.Token);
        }

        /// <summary>
        /// Stops the API host service.
        /// </summary>
        /// <returns>A task representing the asynchronous operation</returns>
        public async Task StopAsync()
        {
            if (_host != null)
            {
                await _host.StopAsync();
            }
        }

        /// <summary>
        /// Creates and configures the web host builder for the API.
        /// </summary>
        /// <returns>A configured host builder</returns>
        private IHostBuilder CreateHostBuilder()
        {
            return Host.CreateDefaultBuilder()
                .ConfigureWebHostDefaults(webBuilder =>
                {
                    webBuilder.UseUrls($"http://localhost:{_port}");
                    webBuilder.Configure(app =>
                    {
                        app.UseRouting();
                        app.UseEndpoints(endpoints =>
                        {
                            endpoints.MapControllers();
                        });
                    });
                    webBuilder.ConfigureServices(services =>
                    {
                        // Register controllers and API-related services
                        services.AddControllers();
                        
                        // Register application services
                        services.AddSingleton(_llmService);
                        services.AddSingleton(_commandMapper);
                        services.AddSingleton<IApiService, ApiService>();
                        services.AddSingleton<IQueryService, QueryService>();
                        
                        // Add CORS support
                        services.AddCors(options =>
                        {
                            options.AddDefaultPolicy(builder =>
                            {
                                builder.AllowAnyOrigin()
                                       .AllowAnyMethod()
                                       .AllowAnyHeader();
                            });
                        });
                    });
                })
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddDebug();
                });
        }

        /// <summary>
        /// Disposes the resources used by the ApiHostService.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the resources used by the ApiHostService.
        /// </summary>
        /// <param name="disposing">True if called from Dispose(), false if called from finalizer</param>
        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _cancellationTokenSource.Cancel();
                    _host?.Dispose();
                    _cancellationTokenSource.Dispose();
                }

                _isDisposed = true;
            }
        }
    }
}