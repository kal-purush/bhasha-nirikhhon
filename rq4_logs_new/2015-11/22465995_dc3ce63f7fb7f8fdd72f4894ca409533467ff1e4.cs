using System.IO;
using System.ServiceProcess;
using ConDep.Dsl.Operations.Application.Deployment.WindowsService;
using ConDep.Dsl.Validation;

namespace ConDep.Dsl.Operations.Remote.Infrastructure.Windows.WindowsService
{
    public class ConfigureWindowsServiceOperation : WindowsServiceOperationBase
    {
        private readonly string _serviceDirPath;

        public ConfigureWindowsServiceOperation(string serviceName, string displayName, string serviceDirPath, string relativeExePath, WindowsServiceOptions.WindowsServiceOptionValues values) : base(serviceName, displayName, relativeExePath, values)
        {
            _serviceDirPath = serviceDirPath;
        }

        public override bool IsValid(Notification notification)
        {
            return true;
        }

        public override string Name { get { return "Configure Windows Service"; } }

        protected override void ConfigureInstallService(IOfferRemoteComposition server)
        {
            var installCmd = string.Format("New-ConDepWinService '{0}' '{1}' {2} {3} {4}",
                                           _serviceName,
                                           Path.Combine(_serviceDirPath, _relativeExePath) + " " + _values.ExeParams,
                                           string.IsNullOrWhiteSpace(_displayName) ? "$null" : ("'" + _displayName + "'"),
                                           string.IsNullOrWhiteSpace(_values.Description)
                                               ? "$null"
                                               : ("'" + _values.Description + "'"),
                                           _values.StartupType.HasValue ? "'" + _values.StartupType + "'" : "'" + ServiceStartMode.Manual + "'"
                );

            server.Execute.PowerShell(installCmd);
        }

    }
}