using AutoMapper;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Jobs;
using CosmosQuery.Benchmarks.Data.Entities;
using CosmosQuery.Benchmarks.Data.Mappings;
using CosmosQuery.Benchmarks.Data.Models;
using CosmosQuery.Benchmarks.Infrastructure;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.OData;
using Microsoft.AspNetCore.OData.Query;
using Microsoft.Extensions.DependencyInjection;
using IConfigurationProvider = AutoMapper.IConfigurationProvider;

namespace CosmosQuery.Benchmarks;

[SimpleJob(RuntimeMoniker.Net60)]
[SimpleJob(RuntimeMoniker.Net70)]
[MemoryDiagnoser]
public class Bench
{
    private const string Query = "/forest";
    private readonly IQueryable<Forest> queryable = new List<Forest>().AsQueryable();
    private IServiceProvider serviceProvider = default!;

    [GlobalSetup]
    public void Setup()
    {
        IServiceCollection services = new ServiceCollection();
        IMvcCoreBuilder builder = new TestMvcCoreBuilder
        {
            Services = services
        };

        builder.AddOData();
        services
            .AddSingleton<IConfigurationProvider>(new MapperConfiguration(cfg => cfg.AddMaps(typeof(IAssemablyMarker).Assembly)))
            .AddTransient<IMapper>(sp => new Mapper(sp.GetRequiredService<IConfigurationProvider>(), sp.GetService))
            .AddTransient<IApplicationBuilder>(sp => new ApplicationBuilder(sp))
            .AddRouting()
            .AddLogging();

        this.serviceProvider = services.BuildServiceProvider();
    }

    [Benchmark]
    public ForestModel[] GetQueryBenchMark()
        => Get<Forest, ForestModel>(this.queryable, Query).ToArray();


    private IQueryable<TModel> Get<TEntity, TModel>(IQueryable<TEntity> queryable, string query, ODataQueryOptions<TModel>? options = null, QuerySettings? querySettings = null)
        where TEntity : class
        where TModel : class
    {
        return
        (
            DoGet
            (
                queryable,
                this.serviceProvider.GetRequiredService<IMapper>()
            )
        );

        IQueryable<TModel> DoGet(IQueryable<TEntity> dataQueryable, IMapper mapper)
        {
            return dataQueryable.GetQuery
            (
                mapper,
                options ?? GetODataQueryOptions<TModel>(query),
                querySettings!
            );
        }
    }

    private ODataQueryOptions<TModel> GetODataQueryOptions<TModel>(string query, string? customNamespace = null)
        where TModel : class
    {
        return ODataHelpers.GetODataQueryOptions<TModel>
        (
            query,
            this.serviceProvider,
            customNamespace
        );
    }
}