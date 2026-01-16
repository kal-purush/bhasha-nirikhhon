using System;
using System.Collections.ObjectModel;
using System.Management.Automation;
using System.Threading;

namespace PowerShellWithCSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
             * example 1: simple excute powershell command
             */
            PowerShell PowerShellInstance1 = PowerShell.Create();
            var cmd01 = "$t = get-date; $t;";
            PowerShellInstance1.AddScript(cmd01);
            Collection<PSObject> psObjects1 = PowerShellInstance1.Invoke();

            foreach (PSObject output in psObjects1)
            {
                if (output != null)
                {
                    Console.WriteLine(output.BaseObject.ToString());
                }
            }

            /*
             * example 2 : excute powershell command with parameter
             */
            PowerShell PowerShellInstance2 = PowerShell.Create();
            var cmd02 = "param($parameter) ; $parameter";
            PowerShellInstance2.AddScript(cmd02);
            PowerShellInstance2.AddParameter("parameter", "Just for testing");
            Collection<PSObject> psObjects2 = PowerShellInstance2.Invoke();

            foreach (PSObject output in psObjects2)
            {
                if (output != null)
                {
                    Console.WriteLine(output.BaseObject.ToString());
                }
            }

            /*
             * example 3 : Excute remote  powershell command
             */
            PowerShell PowerShellInstance3 = PowerShell.Create();
            var cmd03 = "$password=ConvertTo-SecureString -String \"password\" -AsPlainText -Force;" + 
                        "$Cred = New-Object System.Management.Automation.PsCredential(\"user_name\",$password);"+
                        "Invoke-Command -ComputerName computer_name -Credential $Cred {Set-Location -Path \"C:\\apache-jmeter-3.2\\bin\";cmd.exe /c jmeter-server.bat;}";
            PowerShellInstance3.AddScript(cmd03);
            Collection<PSObject> psObjects3 = PowerShellInstance3.Invoke();

            foreach (PSObject output in psObjects3)
            {
                if (output != null)
                {
                    Console.WriteLine(output.BaseObject.ToString());
                }
            }


            /*
             * example 4 : Excute remote  powershell command in background
             */
            PowerShell PowerShellInstance4 = PowerShell.Create();
            var cmd04 = "$password=ConvertTo-SecureString -String \"password\" -AsPlainText -Force;" +
                        "$Cred = New-Object System.Management.Automation.PsCredential(\"user_name\",$password);" +
                        "Invoke-Command -ComputerName computer_name -Credential $Cred {Set-Location -Path \"C:\\apache-jmeter-3.2\\bin\";cmd.exe /c jmeter-server.bat;}";
            PowerShellInstance4.AddScript(cmd04);
            PSDataCollection<PSObject> outputCollection4 = new PSDataCollection<PSObject>();
            IAsyncResult resutlt = PowerShellInstance4.BeginInvoke<PSObject, PSObject>(null, outputCollection4);

            while (resutlt.IsCompleted == false)
            {
                Thread.Sleep(1000);
            }

            foreach (var item in outputCollection4)
            {
                Console.WriteLine(resutlt);
            }

            /*
             * example 5 : Excute remote  powershell command in background + output and status event handler
             */
            PowerShell PowerShellInstance5 = PowerShell.Create();
            var cmd05 = "$password=ConvertTo-SecureString -String \"password\" -AsPlainText -Force;" +
                        "$Cred = New-Object System.Management.Automation.PsCredential(\"user_name\",$password);" +
                        "Invoke-Command -ComputerName computer_name -Credential $Cred {Set-Location -Path \"C:\\apache-jmeter-3.2\\bin\";cmd.exe /c jmeter-server.bat;}";
            PowerShellInstance5.AddScript(cmd05);
            PSDataCollection<PSObject> outputCollection5 = new PSDataCollection<PSObject>();
            outputCollection5.DataAdded += new EventHandler<DataAddedEventArgs>(Output_DataAdded);
            PowerShellInstance5.InvocationStateChanged += new EventHandler<PSInvocationStateChangedEventArgs>(Powershell_InvocationStateChanged);
            PowerShellInstance5.BeginInvoke<PSObject, PSObject>(null, outputCollection5);
        }

        private static void Output_DataAdded(object sender, DataAddedEventArgs e)
        {
            PSDataCollection<PSObject> myp = (PSDataCollection<PSObject>)sender;
            Collection<PSObject> results = myp.ReadAll();
            foreach (PSObject result in results)
            {
                Console.WriteLine(result.ToString());
            }
        }

        private static void Powershell_InvocationStateChanged(object sender, PSInvocationStateChangedEventArgs e)
        {
            Console.WriteLine("PowerShell object state changed: state: {0}\n", e.InvocationStateInfo.State);
        }
    }
}