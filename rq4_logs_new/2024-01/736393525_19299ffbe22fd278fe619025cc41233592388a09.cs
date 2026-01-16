using API.Utils;
using BusinessLogic.Interfaces;
using BusinessLogic.Models.DTOs;
using BusinessLogic.Services;
using BusinessLogic.Utils;
using BusinessLogic.Validation;
using Data.Data;
using Data.Interfaces;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations;
using System.Net.Mime;
using System.Text;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

namespace API
{
    public class Startup
    {
        public IConfiguration Configuration { get; }

        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }
        public void ConfigureServices(IServiceCollection services) 
        {
            #region Database
            /*string connection = Configuration.GetConnectionString("PostgreSQLConnection2")!;

            var optionsBuilder = new DbContextOptionsBuilder<OnlineLibriaryDBContext>().UseNpgsql(connection, sqlServerOptions => sqlServerOptions.EnableRetryOnFailure());

            var options = optionsBuilder.Options;

            using (var context = new OnlineLibriaryDBContext(options))
            {
                var canConnect = false;

                try
                {
                    canConnect = context.Database.CanConnect();

                    context.Users.FirstOrDefault();
                    context.Books.FirstOrDefault();
                    context.Authors.FirstOrDefault();
                    context.Genres.FirstOrDefault();
                }
                catch
                {
                    canConnect = false;
                }

                if (!canConnect)
                {
                    context.Database.EnsureCreated();
                    SeedData(context);
                }
            }

            var unitOfWork = new UnitOfWork(new OnlineLibriaryDBContext(options));

            services.AddDbContext<OnlineLibriaryDBContext>(options =>
            {
                options.UseNpgsql(connection, sqlServerOptions => sqlServerOptions.EnableRetryOnFailure());
            });
            */

            var context = new OnlineLibriaryDBContext(new DbContextOptionsBuilder<OnlineLibriaryDBContext>()
                           .EnableSensitiveDataLogging()
                           .UseInMemoryDatabase(databaseName: "Test_Database").Options, ensureDeleted: true);

            Seeder.SeedData(context);

            var unitOfWork = new UnitOfWork(context);

            services.AddSingleton<IUnitOfWork>(x => unitOfWork);

            services.AddDbContext<OnlineLibriaryDBContext>(options =>
            {
                options.UseInMemoryDatabase("Test_Database");
            });

            #endregion

            #region Utils

            services.AddSingleton<ISecurePasswordHasher, SecurePasswordHasher>();

            services.AddMemoryCache();

            services.AddAutoMapper(typeof(AutomapperProfile).Assembly);

            #endregion

            #region Validators

            services.AddTransient<IValidator<AuthorDTO>, AuthorValidator>();

            services.AddTransient<IValidator<UserDTO>, UserValidator>();

            services.AddTransient<IValidator<GenreDTO>, GenreValidator>();

            services.AddTransient<IValidator<BookDTO>, BookValidator>();

            services.AddTransient<IAuthenticationValidator, AuthenticationValidator>();

            #endregion

            #region Services

            services.AddTransient<ICrud<UserDTO>, UserService>();
           
            services.AddTransient<ICrud<BookDTO>, BookService>();

            services.AddTransient<ICrud<GenreDTO>, GenreService>();

            services.AddTransient<ICrud<AuthorDTO>, AuthorService>();

            services.AddTransient<IAuthenticationService, AuthenticationService>();

            services.AddSingleton<ICacheService, CacheService>();

            #endregion

            services.AddControllers();

            services.AddHealthChecks();

            services.AddEndpointsApiExplorer();

            #region Authentication
            services.AddAuthorization(options =>
            {
                var defaultAuthorizationPolicyBuilder = new AuthorizationPolicyBuilder(
                    JwtBearerDefaults.AuthenticationScheme);

                defaultAuthorizationPolicyBuilder =
                    defaultAuthorizationPolicyBuilder.RequireAuthenticatedUser();

                options.DefaultPolicy = defaultAuthorizationPolicyBuilder.Build();
            });
            services.AddAuthentication(option =>
            {
                option.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                option.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;

            }).AddJwtBearer(options =>
            {
                options.TokenValidationParameters = new TokenValidationParameters
                {
                    ValidateIssuer = true,
                    ValidateAudience = true,
                    ValidateLifetime = false,
                    ValidateIssuerSigningKey = true,
                    ValidIssuer = Configuration["Jwt:Issuer"],
                    ValidAudience = Configuration["Jwt:Audience"],
                    IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(Configuration["Jwt:Key"]))
                };
            });
            #endregion
        }
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints => {
                endpoints.MapControllers();
                endpoints.MapHealthChecks("/healthcheck", new HealthCheckOptions
                {
                    ResponseWriter = async (context, report) =>
                    {
                        var result = JsonConvert.SerializeObject(
                            new
                            {
                                status = report.Status.ToString(),
                                date = DateTime.Now
                            }, Formatting.Indented);
                        context.Response.ContentType = MediaTypeNames.Application.Json;
                        await context.Response.WriteAsync(result);
                    }
                });
            });

        }

    }
}