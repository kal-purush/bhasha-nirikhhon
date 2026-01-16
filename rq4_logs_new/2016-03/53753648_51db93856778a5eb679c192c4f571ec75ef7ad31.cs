using FluentAssertions;
using Ninject;
using Virus.Commands.Tests;
using Xunit;

namespace Virus.Commands.Ninject.Tests
{

    [Trait("", "Resolver tests")]
    public class NinjectResolverTests
    {
        [Fact(DisplayName = "Test ninject resolver")]
        public void ResolverTest()
        {
            SetNinjectCommandResolver()
                .Bind<SimpleCommand>()
                    .To<CommandWithoutInAndOutParameters>();

            var command = CommandsFactory.GetCommand<SimpleCommand>();
            command?.Execute();

            command.Should().NotBeNull();
            CommandWithoutInAndOutParameters.Executed.Should().BeTrue();
        }

        private IKernel SetNinjectCommandResolver()
        {
            var kernel = new StandardKernel();
            var resolver = new NinjectCommandsResolver(kernel);
            CommandsFactory.SetResolver(resolver);
            return kernel;
        }

    }
}