using FluentAssertions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Valour.Api.Items.Messages;
using Xunit;

namespace ValourSharp.Tests;

public class CommandModuleTesting
{
    [Theory]
    [MemberData("ModuleTests")]
    public void TestModuleCheckMerging(CommandModule input, Func<PlanetMessage, Task<bool>>[] checks, CommandModule expected)
    {
        input.CombineChecks(checks).Checks.Should().BeEquivalentTo(expected.Checks);
    }

    public static IEnumerable<object[]> ModuleTests()
    {
        var fake = new CommandModule(null, null, Array.Empty<Func<PlanetMessage, Task<bool>>>());

        var check = (PlanetMessage _) => Task.FromResult(true);

        var fake2 = new CommandModule(null, null, new Func<PlanetMessage, Task<bool>>[1] { check });

        var checks = new Func<PlanetMessage, Task<bool>>[1] { check };

        yield return new object[] { fake, checks, fake2 };
        yield return new object[] { fake, Array.Empty<Func<PlanetMessage, Task<bool>>>(), fake };
    }
}