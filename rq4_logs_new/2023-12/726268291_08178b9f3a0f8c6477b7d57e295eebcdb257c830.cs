using AspNetCoreRateLimit;
using HealthChecks.UI.Client;
using LT.DigitalOffice.DashboardService.Broker;
using LT.DigitalOffice.Kernel.BrokerSupport.Configurations;
using LT.DigitalOffice.Kernel.BrokerSupport.Extensions;
using LT.DigitalOffice.Kernel.BrokerSupport.Middlewares.Token;
using LT.DigitalOffice.Kernel.Configurations;
using LT.DigitalOffice.Kernel.EFSupport.Helpers;
using LT.DigitalOffice.Kernel.Extensions;
using LT.DigitalOffice.Kernel.Middlewares.ApiInformation;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace LT.DigitalOffice.DashboardService;

public class Startup : BaseApiInfo
{
  public const string CorsPolicyName = "LtDoCorsPolicy";

  private readonly RabbitMqConfig _rabbitMqConfig;
  private readonly BaseServiceInfoConfig _serviceInfoConfig;

  public IConfiguration Configuration { get; }

  public Startup(IConfiguration configuration)
  {
    Configuration = configuration;

    _rabbitMqConfig = Configuration
      .GetSection(BaseRabbitMqConfig.SectionName)
      .Get<RabbitMqConfig>();

    _serviceInfoConfig = Configuration
      .GetSection(BaseServiceInfoConfig.SectionName)
      .Get<BaseServiceInfoConfig>();

    Version = "1.0";
    Description = "DashboardService is an API intended to work with dashboards";
    StartTime = DateTime.UtcNow;
    ApiName = $"LT Digital Office - {_serviceInfoConfig.Name}";
  }
  
  public void ConfigureServices(IServiceCollection services)
  {
    services.AddCors(options =>
    {
      options.AddPolicy(
        CorsPolicyName,
        builder =>
        {
          builder
            .AllowAnyOrigin()
            .AllowAnyHeader()
            .AllowAnyMethod();
        });
    });

    services.Configure<TokenConfiguration>(Configuration.GetSection("CheckTokenMiddleware"));
    services.Configure<BaseRabbitMqConfig>(Configuration.GetSection(BaseRabbitMqConfig.SectionName));
    services.Configure<BaseServiceInfoConfig>(Configuration.GetSection(BaseServiceInfoConfig.SectionName));

    services.AddHttpContextAccessor();
    services
      .AddControllers()
      .AddJsonOptions(options =>
      {
        options.JsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
      })
      .AddNewtonsoftJson(options =>
      {
        options.SerializerSettings.DateParseHandling = DateParseHandling.None;
      });

    string connStr = ConnectionStringHandler.Get(Configuration);
    
    // TODO: Add db context here
    // services.AddDbContext<HERE>(options =>
    // {
    //   options.UseSqlServer(connStr);
    // });

    services.AddBusinessObjects();

    services.ConfigureMassTransit(_rabbitMqConfig);

    services.AddMemoryCache();

    services.Configure<IpRateLimitOptions>(options =>
      Configuration.GetSection("IpRateLimitingSettings").Bind(options));

    services.AddSingleton<IRateLimitConfiguration, RateLimitConfiguration>();

    services.AddInMemoryRateLimiting();

    services
      .AddHealthChecks()
      .AddSqlServer(connStr)
      .AddRabbitMqCheck();
  }

  public void Configure(IApplicationBuilder app, ILoggerFactory loggerFactory)
  {
    //TODO: Add db context here
    //app.UpdateDatabase<HERE>();

    app.UseForwardedHeaders();

    app.UseExceptionsHandler(loggerFactory);

    app.UseApiInformation();

    app.UseRouting();

    app.UseMiddleware<TokenMiddleware>();

    app.UseCors(CorsPolicyName);

    app.UseIpRateLimiting();

    app.UseEndpoints(endpoints =>
    {
      endpoints.MapControllers().RequireCors(CorsPolicyName);

      endpoints.MapHealthChecks($"/hc", new HealthCheckOptions
      {
        ResultStatusCodes = new Dictionary<HealthStatus, int>
        {
          { HealthStatus.Unhealthy, 200 },
          { HealthStatus.Healthy, 200 },
          { HealthStatus.Degraded, 200 },
        },
        Predicate = check => check.Name != "masstransit-bus",
        ResponseWriter = UIResponseWriter.WriteHealthCheckUIResponse
      });
    });
  }
}