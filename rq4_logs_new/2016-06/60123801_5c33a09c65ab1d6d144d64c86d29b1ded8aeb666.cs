using System;
using System.ComponentModel.Design;
using System.Linq;
using System.Runtime.InteropServices;
using EnvDTE;
using Microsoft.VisualStudio;
using Microsoft.VisualStudio.Shell;

namespace Topres.VSDebugAttacher
{
    //// This attribute tells the PkgDef creation utility (CreatePkgDef.exe) that this class is a package.
    [PackageRegistration(UseManagedResourcesOnly = true)]
    //// This attribute is used to register the informations needed to show the this package in the Help/About dialog of Visual Studio.
    [InstalledProductRegistration("#110", "#112", "1.0", IconResourceID = 400)]
    //// This attribute is needed to let the shell know that this package exposes some menus.
    [ProvideMenuResource("Menus.ctmenu", 1)]
    [Guid(GuidList.guidAttachToPkgString)]
    [ProvideOptionPage(typeof(GeneralOptionsPage), "Topres.VSDebugAttacher", "General", 110, 120, false)]
    [ProvideAutoLoad(VSConstants.UICONTEXT.SolutionExists_string)]
    [ProvideAutoLoad(VSConstants.UICONTEXT.NoSolution_string)]
    public sealed class AttachToPackage : Package
    {
        protected override void Initialize()
        {
            base.Initialize();

            OleMenuCommandService mcs = GetService(typeof(IMenuCommandService)) as OleMenuCommandService;

            this.AddAttachToCommand(mcs, PackageCommandIdList.AttachToCustomExe1, gop => gop.ShowAttachToCustomExe1);
            this.AddAttachToCommand(mcs, PackageCommandIdList.AttachToCustomExe2, gop => gop.ShowAttachToCustomExe2);
            this.AddAttachToCommand(mcs, PackageCommandIdList.AttachToCustomExe3, gop => gop.ShowAttachToCustomExe3);
            this.AddAttachToCommand(mcs, PackageCommandIdList.AttachToIISCommandId, gop => gop.ShowAttachToIIS, "w3wp.exe");
            this.AddAttachToCommand(mcs, PackageCommandIdList.AttachToIISExpressCommandId, gop => gop.ShowAttachToIISExpress, "iisexpress.exe");
            this.AddAttachToCommand(mcs, PackageCommandIdList.AttachToNUnitCommandId, gop => gop.ShowAttachToNUnit, "nunit-agent.exe", "nunit.exe", "nunit-console.exe", "nunit-agent-x86.exe", "nunit-x86.exe", "nunit-console-x86.exe");
        }

        private void AddAttachToCommand(OleMenuCommandService mcs, uint commandId, Func<GeneralOptionsPage, bool> isVisible, params string[] programsToAttach)
        {
            var options = (GeneralOptionsPage) this.GetDialogPage(typeof(GeneralOptionsPage));
            OleMenuCommand menuItemCommand = new OleMenuCommand(
                delegate(object sender, EventArgs e)
                {
                    DTE dte = (DTE)this.GetService(typeof(DTE));
                    foreach (Process process in dte.Debugger.LocalProcesses)
                    {
                        if (programsToAttach.Any(p => process.Name.EndsWith(p)))
                        {
                            process.Attach();
                        }

                        if (!programsToAttach.Any())
                        {
                            string processName = null;

                            if (commandId == PackageCommandIdList.AttachToCustomExe1)
                            {
                                processName = options.CustomExe1Name;
                            }

                            if (commandId == PackageCommandIdList.AttachToCustomExe2)
                            {
                                processName = options.CustomExe2Name;
                            }

                            if (commandId == PackageCommandIdList.AttachToCustomExe3)
                            {
                                processName = options.CustomExe2Name;
                            }

                            if (!string.IsNullOrWhiteSpace(processName) &&
                                process.Name.ToLower().Contains(processName.ToLower()))
                            {
                                process.Attach();
                            }
                        }
                    }
                },
                new CommandID(GuidList.guidAttachToCmdSet, (int)commandId));

            menuItemCommand.BeforeQueryStatus += (s, e) =>
            {
                menuItemCommand.Visible = isVisible(options);
            };

            mcs.AddCommand(menuItemCommand);
        }
    }
}