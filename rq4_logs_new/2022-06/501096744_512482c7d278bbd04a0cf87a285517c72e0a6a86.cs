using FoodZone.Data;
using FoodZone.Data.Infrastructure;
using FoodZone.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
//add connection string
builder.Services.AddDbContext<FoodZoneContext>(opt =>
{
    opt.UseSqlServer(builder.Configuration.GetConnectionString("FoodZoneCnn"));
});

builder.Services.AddAuthentication();
builder.Services.ConfigureIdentity();

//add dependency injection
builder.Services.AddScoped<IUnitOfWork, UnitOfWork>();

builder.Services.AddControllers();

//add cors
builder.Services.AddCors(o =>
{
    o.AddPolicy("AllowAll", bder => bder.AllowAnyOrigin()
                                                           .AllowAnyMethod()
                                                           .AllowAnyHeader());
});

builder.Services.AddSwaggerGen(c => c.SwaggerDoc("v1", new OpenApiInfo { Title = "FoodZone", Version = "v1" }));
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors("AllowAll");

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();