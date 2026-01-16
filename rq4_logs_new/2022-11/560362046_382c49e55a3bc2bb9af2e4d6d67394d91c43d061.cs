using Lib.AspNetCore.ServerSentEvents;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddServerSentEvents();
builder.Services.AddHostedService<ServerSideEventsWorker>();

var app = builder.Build();
app.MapServerSentEvents("/sse-endpoint");

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast = Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast")
.WithOpenApi();

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);

    public override string ToString() => $"{Date:d}: {TemperatureC}C, {Summary}";

}

public class ServerSideEventsWorker : IHostedService
{
    private readonly IServerSentEventsService _serverSentEventsService;

    public ServerSideEventsWorker(IServerSentEventsService serverSentEventsService)
    {
        _serverSentEventsService = serverSentEventsService;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };
        while (!cancellationToken.IsCancellationRequested)
        {
            var clients = _serverSentEventsService.GetClients();
            if (clients.Any())
            {
                var forecast = Enumerable.Range(1, 5).Select(index =>
                    new WeatherForecast
                (
                    DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                    Random.Shared.Next(-20, 55),
                    summaries[Random.Shared.Next(summaries.Length)]
                )).ToArray();
                await _serverSentEventsService.SendEventAsync(
                    new ServerSentEvent
                    {
                        Id = Guid.NewGuid().ToString(),
                        Type = "forecast",
                        Data = new List<string> { forecast.ToString() }
                    },
                    cancellationToken
                );
            }

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}