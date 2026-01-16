using GiftSuggester.Core;
using GiftSuggester.Data;
using GiftSuggester.Web;
using GiftSuggester.Web.Middlewares;

var builder = WebApplication.CreateBuilder(args);

builder.Services
    .AddData(builder.Configuration)
    .AddCore()
    .AddWeb();

var app = builder.Build();

app.UseMiddleware<ExceptionMiddleware>();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapControllers();

app.Run();