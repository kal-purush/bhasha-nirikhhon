var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllersWithViews();  // Make sure MVC views are added

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();  // Ensure static files like CSS are served

app.UseRouting();

app.UseAuthorization();

// **First Route for MVC Controller (Home/Index)**:
app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");  // Set Home as the default route

// **Second Route for API Controllers**:
app.MapControllers();  // Maps your API controllers (e.g., WeatherForecast)

app.Run();