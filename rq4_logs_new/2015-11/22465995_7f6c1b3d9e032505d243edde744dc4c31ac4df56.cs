using System.Security.AccessControl;
using ConDep.Dsl.Validation;

namespace ConDep.Dsl.Operations.Remote.Infrastructure.Windows.Acl
{
    internal class AclOperation : RemoteCompositeOperation
    {
        private readonly string _user;
        private readonly string _fileOrFolder;
        private readonly FileSystemRights _accessRights;
        private readonly AclOptions.AclOptionsValues _options;


        public AclOperation(string user, string fileOrFolder, FileSystemRights accessRights, AclOptions.AclOptionsValues options)
        {
            _user = user;
            _fileOrFolder = fileOrFolder;
            _accessRights = accessRights;
            _options = options;
        }

        public override void Configure(IOfferRemoteComposition server)
        {
            server.Execute.PowerShell(string.Format(@"
$inherit = [system.security.accesscontrol.InheritanceFlags]""{0}""
$propagation = [system.security.accesscontrol.PropagationFlags]""{1}""

$directory = ""{2}""
$acl = Get-Acl $directory
$accessRule = New-Object System.Security.AccessControl.FileSystemAccessRule(""{3}"", ""{4}"", $inherit, $propagation, ""{5}"")
$acl.AddAccessRule($accessRule)
Set-Acl -aclobject $acl $directory
", _options.Inheritance, _options.Propagation, _fileOrFolder, _user, _accessRights, _options.Type));
        }

        public override bool IsValid(Notification notification)
        {
            return true;
        }

        public override string Name { get { return "Acl"; } }
    }
}