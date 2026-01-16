using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Virus.Commands.Tests.Fixtures
{
    public class CommandsFactoryFixture
    {

        private Mock<CommandsResolver> resolverMock;

        public CommandsFactoryFixture()
        {
            resolverMock = new Mock<CommandsResolver>();
            CommandsFactory.SetResolver(resolverMock.Object);
        }

        public void SetupBindAbstractCommandToHisImplementation<TAbs, TImpl>(Func<TImpl> createInstance) where TImpl : TAbs
        {
            resolverMock.Setup(resolver => resolver.GetCommand<TAbs>()).Returns(() => createInstance());
        }

    }
}