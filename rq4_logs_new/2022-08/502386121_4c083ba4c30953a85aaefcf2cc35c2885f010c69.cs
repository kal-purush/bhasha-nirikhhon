using Microsoft.Extensions.DependencyInjection;
using Microsoft.OpenApi.Models;
using Ocelot.DependencyInjection;
using Ocelot.Middleware;

var builder = WebApplication.CreateBuilder(args);

builder.Configuration.AddJsonFile("ocelot.json");

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddOcelot();
builder.Services.AddSwaggerForOcelot(builder.Configuration);
builder.Services.AddCacheManager();


var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();
app.UseSwaggerForOcelotUI(opt =>
{
    opt.PathToSwaggerGenerator = "/swagger/docs";
    opt.DownstreamSwaggerHeaders = new[]
    {
        new KeyValuePair<string, string>("Auth-Key", "AuthValue"),
    };
});

app.UseHttpsRedirection();


app.UseOcelot();

app.UseAuthorization();

app.MapControllers();

app.Run();