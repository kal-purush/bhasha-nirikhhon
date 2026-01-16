using Serilog;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Host.UseSerilog();

WebApplication app = builder.Build();

app.UseHttpsRedirection().UseExceptionHandler("/error");