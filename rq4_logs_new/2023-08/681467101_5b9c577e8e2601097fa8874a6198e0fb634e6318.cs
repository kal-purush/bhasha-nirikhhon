using Gamestore.api.Data;
using Gamestore.api.ENDPOINTS;

var builder = WebApplication.CreateBuilder(args);
builder.Services.Addreposiatrires(builder.Configuration);
 
 var app= builder.Build();
await app.Services.InitializeDb();
       app.MapgameEndpoints();
       
    app.Run();
    
 