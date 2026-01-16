
using Messaging.Buffer.Service;
using Microsoft.AspNetCore.Builder;
using WebAppExample.Requests;

namespace WebAppExample
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
            // Add services to the container.
            services.AddRazorPages();
            services.AddControllers();
            services.AddMvc(o => o.EnableEndpointRouting = false);

            services.AddMessagingBuffer(Configuration, "Redis");

            services.AddBuffer<HelloWorldRequestBuffer, HelloWorldRequest, HelloWorldResponse>();
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, IHostApplicationLifetime applicationLifetime)
        {
            app.UseHttpsRedirection();
            app.UseStaticFiles();

            app.UseRouting();

            app.UseAuthorization();
            app.UseMvc();
        }
    }
}