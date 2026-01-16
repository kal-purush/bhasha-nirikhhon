using System.Reflection;
using Autofac;
using Autofac.Core.Registration;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Configuration;
using Nameless.Autofac;

namespace Nameless.Caching.InMemory.DependencyInjection {
    public sealed class CachingModule : ModuleBase {
        #region Private Constants

        private const string MEMORY_CACHE_TOKEN = $"{nameof(MemoryCache)}::e69a9cb8-f681-476f-935a-d9c2d634947c";

        #endregion

        #region Public Constructors

        public CachingModule()
            : base(Array.Empty<Assembly>()) { }

        #endregion

        #region Protected Override Methods

        protected override void Load(ContainerBuilder builder) {
            builder
                .Register(ResolveMemoryCache)
                .Named<IMemoryCache>(MEMORY_CACHE_TOKEN)
                .SingleInstance();

            builder
                .Register(ResolveCache)
                .As<ICache>()
                .SingleInstance();

            base.Load(builder);
        }

        #endregion

        #region Private Static Methods

        private static MemoryCacheOptions GetMemoryCacheOptions(IComponentContext ctx) {
            var configuration = ctx.ResolveOptional<IConfiguration>();
            var options = configuration?
                .GetSection(nameof(MemoryCacheOptions).RemoveTail(new[] { "Options" }))
                .Get<MemoryCacheOptions>();

            return options ?? new();
        }

        private static IMemoryCache ResolveMemoryCache(IComponentContext ctx) {
            var memoryCache = GetMemoryCacheOptions(ctx);
            var result = new MemoryCache(memoryCache);

            return result;
        }

        private static ICache ResolveCache(IComponentContext ctx) {
            var memoryCache = ctx.ResolveNamed<IMemoryCache>(MEMORY_CACHE_TOKEN);
            var result = new InMemoryCache(memoryCache);

            return result;
        }

        #endregion
    }

    public static class ContainerBuilderExtension {
        #region Public Static Methods

        public static IModuleRegistrar AddCaching(this ContainerBuilder self)
            => self.RegisterModule<CachingModule>();

        #endregion
    }
}