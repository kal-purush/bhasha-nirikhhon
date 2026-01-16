using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Management.Automation;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Collections.ObjectModel;
using Util;
using System.Threading;

namespace ZTester.Services
{
    class PowerShellService
    {

        public void ExecutePowerShellScript(string script)
        {
            using (PowerShell PowerShellInstance = PowerShell.Create())
            {
                PowerShellInstance.AddScript(script);
                var resultShell = PowerShellInstance.Invoke();

                foreach (PSObject outputItem in resultShell)
                {
                    if (outputItem != null)
                    {
                        Console.WriteLine(outputItem.BaseObject.ToString() + "\n");
                    }
                }
            }
            Thread.Sleep(2000);
        }
    }
}