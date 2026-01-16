using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using Microsoft.OpenApi.Models;
using System.IdentityModel.Tokens.Jwt;
using System.Text;
using Website_Ecommerce.API.Data;
using Website_Ecommerce.API.Queries;
using Website_Ecommerce.API.Repositories;
using Website_Ecommerce.API.services;

namespace PBL4.WebAPI
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }
        readonly string MyAllowSpecificOrigins = "_myAllowSpecificOrigins";


        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddCors(options =>
            {
                options.AddPolicy("CorsPolicy",
                    builder =>
                    {
                        builder.WithOrigins("http://localhost:4200")
                            .AllowAnyHeader()
                            .AllowAnyMethod()
                            .AllowCredentials();
                    });
            });
            services.AddSignalR();
            services.Configure<Audience>(Configuration.GetSection("Audience"));
            services.AddControllers();
            services.AddHttpContextAccessor();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "PBL4.WebAPI", Version = "v1" });
                //add frame to get token bearer
                c.AddSecurityDefinition("Bearer",
                   new OpenApiSecurityScheme
                   {
                       In = ParameterLocation.Header,
                       Description = "Please enter into field the word 'Bearer' following by space and JWT",
                       Name = "Authorization",
                       Type = SecuritySchemeType.ApiKey,
                       Scheme = "MyAuthKey"
                   });
                c.AddSecurityRequirement(new OpenApiSecurityRequirement()
                {
                    {
                        new OpenApiSecurityScheme
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.SecurityScheme,
                                Id = "Bearer"
                            },
                            Scheme = "Bearer",
                            Name = "Bearer",
                            In = ParameterLocation.Header,
                        },
                        new List<string>()
                    }
                });
            });
            
            var serverVersion = new MySqlServerVersion(new Version(8, 0, 30));
            services.AddDbContext<DataContext>(
                dbContextOptions => 
                {
                    dbContextOptions
                    .UseMySql(Configuration["ConnectionString"], serverVersion)
                    .LogTo(Console.WriteLine, LogLevel.Information)
                    .EnableSensitiveDataLogging()
                    .EnableDetailedErrors();
                }, ServiceLifetime.Scoped);
            
            // services.AddDbContext<DataContext>(options =>
            // {
            //     options.UseSqlServer(Configuration["ConnectionString"]);
            // }, ServiceLifetime.Scoped);
            //set up dependency entity
            //set up DI Repository
            services.AddTransient<IIdentityServices, IdentityServices>();
            services.AddTransient<IProductRepository, ProductRepository>();
            services.AddTransient<ICategroyRepository, CategroyRepository>();
            services.AddTransient<IUserRepository, UserRepository>();


            services.AddTransient<IIdentityServices, IdentityServices>();
            services.AddTransient<IAppQueries>(x=> new AppQueries(Configuration["ConnectionString"]));

            // //set up mediator
            // services.AddMediatR(Assembly.GetExecutingAssembly());
            // // Assembly để .net vào bin và get file dll của những command DI với nhau không cần khai báo tất cả
            // services.AddMediatR(Assembly.GetExecutingAssembly(), typeof(AddCategoryCommand).Assembly);
            // //chỉ lấy những file dll like addcategoryCommand

            //setup validator for fluent validator
            // AssemblyScanner
            //     .FindValidatorsInAssembly(typeof(AddCategoryCommandValidator).Assembly)
            //     .ForEach(item => services.AddScoped(item.InterfaceType, item.ValidatorType));

            // services.AddScoped(typeof(IPipelineBehavior<,>), typeof(ValidatorBehavior<,>));


            var audience = Configuration.GetSection("Audience").Get<Audience>();

            JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Remove("sub");

            var signingKey = new SymmetricSecurityKey(Encoding.ASCII.GetBytes(audience.Secret));
            var tokenValidationParameters = new TokenValidationParameters //verify token
            {
                ValidateIssuerSigningKey = true,
                IssuerSigningKey = signingKey,
                ValidateIssuer = true,
                ValidIssuer = audience.Issuer,
                ValidateAudience = true,
                ValidAudience = audience.Name,
                ValidateLifetime = true,
                ClockSkew = TimeSpan.Zero,
                RequireExpirationTime = true,
            };
            services.AddAuthentication()
                .AddJwtBearer("MyAuthKey", options =>
                {
                    options.RequireHttpsMetadata = false;
                    options.SaveToken = true;
                    options.TokenValidationParameters = tokenValidationParameters;
                });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseSwagger();
                
                app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "PBL4.WebAPI v1"));
                /*app.UseSwaggerUI(c=> {
                    c.DisplayRequestDuration();
                    c.SwaggerEndpoint("/swagger/v1/swagger.json", "PBL4.WebAPI v1");
                });*/
            }

            app.UseRouting();

            app.UseCors("CorsPolicy"); //applie doamin allow config above cross origin request

            app.UseAuthentication();

            app.UseAuthorization();


            

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
                //.RequireCors(MyAllowSpecificOrigins);
                // endpoints.MapHub<PBL4Hub>("/signalr");
            });
        }
    }
}