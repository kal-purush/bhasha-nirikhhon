using Microsoft.OpenApi.Models;
using Igtampe.Neco.Data;
using Igtampe.Neco.Common;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options => {
    options.SwaggerDoc("V2", new OpenApiInfo {
        Version = "V2", Title = "Neco API",
        Description = "A New API with a new domain after the reset in development",
        //TermsOfService = new Uri("https://example.com/terms"),
        Contact = new OpenApiContact {
            Name = "Chopo",
            Url = new Uri("https://twitter.com/igtampe"),
            Email = "igtampe@gmail.com",
        },
        License = new OpenApiLicense {
            Name = "CC0",
            //Url = new Uri("https://example.com/license") //TODO: Actually specify the license once this is done
        }
    });
    options.IncludeXmlComments("./Neco.API.xml");
});

builder.Services.AddDbContext<NecoContext>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment()) {
    app.UseSwagger();
    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/V2/swagger.json", "Neco API"));
    app.UseDeveloperExceptionPage();
}

app.UseHttpsRedirection();
app.UseAuthorization();

app.MapControllers();

app.Run();