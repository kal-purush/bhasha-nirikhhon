using MediatR;
using Microsoft.Azure.Cosmos;
using Microsoft.OpenApi.Models;
using Noteflix.Application.Commands;
using Noteflix.Application.Models;
using Noteflix.Application.Queries;
using Noteflix.Core.Entities;
using Noteflix.Core.Repositories;
using Noteflix.Core.UnitOfWork;
using Noteflix.Infrastructure.AppSettings;
using Noteflix.Infrastructure.Context;
using Noteflix.Infrastructure.Context.Interfaces;
using Noteflix.Infrastructure.Extensions;
using Noteflix.Infrastructure.Repositories;
using Noteflix.Infrastructure.UnitOfWork;

namespace Noteflix.API
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            var cOpts = new CosmosClientOptions
            {
                SerializerOptions = new CosmosSerializationOptions()
                {
                    PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase,
                    IgnoreNullValues = true
                }
            };

            CosmosDbSettings cosmosDbConfig = Configuration.GetSection("ConnectionStrings:CosmosDBConnection").Get<CosmosDbSettings>();

            services.AddCosmosDb(cosmosDbConfig.EndpointUrl,
                                 cosmosDbConfig.PrimaryKey,
                                 cosmosDbConfig.DatabaseName,
                                 cosmosDbConfig.Containers);

            services.AddScoped<INotebooksContainerContext, NotebooksContainerContext>()
                .AddScoped<INoteRepository, NoteRepository>()
                .AddScoped<INotebookUnitOfWork, NotebookUnitOfWork>()
                .AddMediatR(typeof(NoteEntity), typeof(CreateNoteCommand));

            services.AddRouting(options => options.LowercaseUrls = true);
            services.AddControllers();
            services.AddEndpointsApiExplorer();
            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = "Noteflix.API", Version = "v1" });
            });
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseSwagger();
                app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "Noteflix.API v1"));
            }

            app.UseHttpsRedirection();

            app.UseRouting();

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                MapEndpoints(endpoints);
            });
        }

        private void MapEndpoints(IEndpointRouteBuilder endpoints)
        {
            endpoints.MapGet("/note", async (IMediator mediator) =>
            {
                var notes = await mediator.Send(new ReadAllNotesQuery());
                return notes;
            });

            endpoints.MapPost("/note", async (CreateNoteDto request, IMediator mediator) =>
            {
                var command = new CreateNoteCommand
                {
                    Title = request.Title,
                    Content = request.Content,
                    CreatedBy = request.CreatedBy,
                    Tasks = request.Tasks,
                };

                var note = await mediator.Send(command);
                return note;
            });
        }
    }
}