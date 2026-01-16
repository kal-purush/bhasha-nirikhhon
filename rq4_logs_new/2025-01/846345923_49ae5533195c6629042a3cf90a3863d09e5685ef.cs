using System.Threading;
using F2.Src;
using F2.Src.Presentation;
using FastEndpoints;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);

// Setup services
var services = builder.Services;
services.AddFastEndpoints(config => config.Assemblies = [typeof(F2Endpoint).Assembly]);

F2Register.RegisterF2(services, builder.Configuration);

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseFastEndpoints();
}

if (app.Environment.IsStaging())
{
    app.UseFastEndpoints();
}

if (app.Environment.IsProduction())
{
    app.UseFastEndpoints();
}

await app.RunAsync(CancellationToken.None);