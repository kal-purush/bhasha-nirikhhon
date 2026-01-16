using System;
using System.Collections.Generic;
using System.Reflection;
using ArmaForces.ArmaServerManager.Discord.Configuration;
using ArmaForces.ArmaServerManager.Discord.Extensions;
using ArmaForces.ArmaServerManager.Discord.Features;
using ArmaForces.ArmaServerManager.Discord.Features.Mods;
using ArmaForces.ArmaServerManager.Discord.Features.Server;
using ArmaForces.ArmaServerManager.Discord.Features.Server.DTOs;
using ArmaForces.ArmaServerManager.Discord.Features.ServerConfig;
using FluentAssertions;
using Xunit;

namespace ArmaForces.ArmaServerManager.Discord.Tests
{
    public class VisibilityTests
    {
        [Fact]
        public void VisibilityTests_AllTypes_OnlyExpectedArePublic()
        {
            var expectedPublicTypes = new List<Type>
            {
                typeof(IManagerConfiguration),
                typeof(CommandServiceExtensions),
                typeof(ServiceCollectionExtensions),
                typeof(IModsManagerClient),
                typeof(ModsModule),
                typeof(IServerManagerClient),
                typeof(ServerModule),
                typeof(ServerStartRequest),
                typeof(ServerStatus),
                typeof(IConfigurationManagerClient),
                typeof(ServerConfigModule),
                typeof(ManagerModuleBase)
            };

            var visibleTypes = Assembly.GetAssembly(typeof(ServerModule))?.ExportedTypes;

            visibleTypes.Should().BeEquivalentTo(expectedPublicTypes);
        }
    }
}