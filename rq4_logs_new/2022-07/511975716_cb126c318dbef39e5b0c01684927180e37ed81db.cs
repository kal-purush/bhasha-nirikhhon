using NimmalaWeek6.Models;
using Microsoft.EntityFrameworkCore;


var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllersWithViews();

//Get access to the connecting string object through configurstion settings
IConfiguration configuration = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

//DI passes the connstring to the context object with correct output directory path

builder.Services.AddDbContext<HogwartsContext>(option =>
Option.UseSqlServer(configuration.GetConnectionString("HogwartsConnString")));



var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();

app.UseRouting();

app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();