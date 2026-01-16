using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyModel;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace NetCoreUtils.Database.MultiTenancy
{
    // from: https://gunnarpeipman.com/ef-core-global-query-filters/
    public class MultiTenantContext : DbContext
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            foreach (var type in GetEntityTypes())
            {
                var method = SetGlobalQueryMethod.MakeGenericMethod(type);
                method.Invoke(this, new object[] { modelBuilder });
            }

            base.OnModelCreating(modelBuilder);
        }

        static readonly MethodInfo SetGlobalQueryMethod = typeof(MultiTenantContext).GetMethods(BindingFlags.Public | BindingFlags.Instance)
                                                                                    .Single(t => t.IsGenericMethod && t.Name == "SetGlobalQuery");

        public void SetGlobalQuery<T>(ModelBuilder builder) where T : TenantEntity
        {
            // see: https://docs.microsoft.com/en-us/ef/core/querying/filters
            builder.Entity<T>().HasQueryFilter(e => e.TenantId == _tenantId);
        }

        private static IList<Type> _entityTypeCache;
        private string _tenantId;

        public MultiTenantContext([NotNullAttribute] DbContextOptions options, ITenantProvider tenantProvider) : base(options)
        {
            _tenantId = tenantProvider.GetTenantId();
        }

        private static IList<Type> GetEntityTypes()
        {
            if (_entityTypeCache != null)
            {
                return _entityTypeCache.ToList();
            }

            _entityTypeCache = (from a in GetReferencingAssemblies()
                                from t in a.DefinedTypes
                                where t.BaseType == typeof(TenantEntity)
                                select t.AsType()).ToList();

            return _entityTypeCache;
        }

        private static IEnumerable<Assembly> GetReferencingAssemblies()
        {
            var assemblies = new List<Assembly>();
            var dependencies = DependencyContext.Default.RuntimeLibraries;

            foreach (var library in dependencies)
            {
                try
                {
                    var assembly = Assembly.Load(new AssemblyName(library.Name));
                    assemblies.Add(assembly);
                }
                catch (FileNotFoundException)
                {
                    // TODO: print to console or log 
                }
            }
            return assemblies;
        }
    }
}