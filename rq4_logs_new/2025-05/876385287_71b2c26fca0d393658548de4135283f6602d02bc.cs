using LangSharp.Core.Commands;
using LangSharp.Core.Enums;
using LangSharp.Core.Interfaces.Handlers;
using LangSharp.Core.Services;
using Moq;

namespace LangSharp.UnitTests.Core.Services
{
    public class LangSharpServiceTests
    {
        private readonly Mock<IRequestValidatorHandler> _requestValidatorHandler;
        private readonly Mock<IConfigurationSetupHandler> _configurationSetupHandler;
        private readonly Mock<ISetEnvironmentVariablesHandler> _setEnvironmentVariablesHandler;
        private readonly Mock<IVirtualEnvironmentHandler> _virtualEnvironmentHandler;
        private readonly Mock<IPythonInstallationCheckerHandler> _pythonInstallationCheckerHandler;
        private readonly Mock<IPythonInitializerHandler> _pythonInitializerHandler;
        private readonly Mock<IPythonDependenciesInstallerHandler> _pythonDependenciesInstallerHandler;
        private readonly Mock<ICommandExecutionHandler> _commandExecutionHandler;
        private readonly LangSharpService _langSharpService;

        public LangSharpServiceTests()
        {
            _requestValidatorHandler = new Mock<IRequestValidatorHandler>();
            _configurationSetupHandler = new Mock<IConfigurationSetupHandler>();
            _setEnvironmentVariablesHandler = new Mock<ISetEnvironmentVariablesHandler>();
            _virtualEnvironmentHandler = new Mock<IVirtualEnvironmentHandler>();
            _pythonInstallationCheckerHandler = new Mock<IPythonInstallationCheckerHandler>();
            _pythonInitializerHandler = new Mock<IPythonInitializerHandler>();
            _pythonDependenciesInstallerHandler = new Mock<IPythonDependenciesInstallerHandler>();
            _commandExecutionHandler = new Mock<ICommandExecutionHandler>();

            _requestValidatorHandler
                .Setup(x => x.SetNext(_configurationSetupHandler.Object))
                .Returns(_configurationSetupHandler.Object);
            _configurationSetupHandler
                .Setup(x => x.SetNext(_setEnvironmentVariablesHandler.Object))
                .Returns(_setEnvironmentVariablesHandler.Object);
            _setEnvironmentVariablesHandler
                .Setup(x => x.SetNext(_pythonInstallationCheckerHandler.Object))
                .Returns(_pythonInstallationCheckerHandler.Object);
            _pythonInstallationCheckerHandler
                .Setup(x => x.SetNext(_pythonInitializerHandler.Object))
                .Returns(_pythonInitializerHandler.Object);
            _pythonInitializerHandler
                .Setup(x => x.SetNext(_virtualEnvironmentHandler.Object))
                .Returns(_virtualEnvironmentHandler.Object);
            _virtualEnvironmentHandler
                .Setup(x => x.SetNext(_pythonDependenciesInstallerHandler.Object))
                .Returns(_pythonDependenciesInstallerHandler.Object);
            _pythonDependenciesInstallerHandler
                .Setup(x => x.SetNext(_commandExecutionHandler.Object))
                .Returns(_commandExecutionHandler.Object);

            _langSharpService = new LangSharpService(
                _requestValidatorHandler.Object,
                _configurationSetupHandler.Object,
                _setEnvironmentVariablesHandler.Object,
                _virtualEnvironmentHandler.Object,
                _pythonInstallationCheckerHandler.Object,
                _pythonInitializerHandler.Object,
                _pythonDependenciesInstallerHandler.Object,
                _commandExecutionHandler.Object
            );
        }

        [Fact]
        public async Task CallAIChatAsync_ShouldCallHandleOnCommandExecutionHandler()
        {
            // Arrange
            var prompt = "Hello, AI!";
            var expectedResponse = "AI Response";
            _requestValidatorHandler
                .Setup(x => x.Handle(It.Is<CommandRequest>(r => r.CommandType == TypeCommand.GetResponse && r.Parameter == prompt)))
                .Returns(expectedResponse);

            // Act
            var result = await _langSharpService.CallAIChatAsync(prompt);

            // Assert
            _requestValidatorHandler.Verify(x => x.Handle(It.Is<CommandRequest>(r =>
                r.CommandType == TypeCommand.GetResponse && r.Parameter == prompt)), Times.Once);
        }

        [Fact]
        public async Task ExecuteDatabaseQueryAsync_ShouldCallHandleOnCommandExecutionHandler()
        {
            // Arrange
            var query = "SELECT * FROM Users";
            var expectedResponse = "Query Result";
            _requestValidatorHandler
                .Setup(x => x.Handle(It.Is<CommandRequest>(r => r.CommandType == TypeCommand.ExecuteDatabaseQuery && r.Parameter == query)))
                .Returns(expectedResponse);

            // Act
            var result = await _langSharpService.ExecuteDatabaseQueryAsync(query);

            // Assert
            _requestValidatorHandler.Verify(x => x.Handle(It.Is<CommandRequest>(r =>
                r.CommandType == TypeCommand.ExecuteDatabaseQuery && r.Parameter == query)), Times.Once);
        }
    }
}