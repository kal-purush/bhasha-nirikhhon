using Insane.AspNet;
using Insane.AspNet.Identity.Model1.Context;
using Insane.EntityFrameworkCore;
using Insane.WebApiTest;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Insane.WebApiLiveTest.Factory
{
    public abstract class IdentityDbContextFactoryBase<TContextBase, TContext> : DbContextFactoryBase<TContextBase, TContext>
         where TContext : TContextBase
        where TContextBase : CoreDbContextBase
    {
        public override string ConfigurationFilename { get; } = AspNetConstants.DefaultConfigurationFile;
        public override string ConfigurationPath { get;  } = "InsaneIdentity:DbContextSettings";
        public override Type UserSecretsType { get;  } = typeof(Program);

        public override Action<DbContextOptionsBuilder> DbContextOptionsBuilderAction => (options) =>
        {
            options.EnableDetailedErrors();
            options.EnableSensitiveDataLogging();
        };
    }
}