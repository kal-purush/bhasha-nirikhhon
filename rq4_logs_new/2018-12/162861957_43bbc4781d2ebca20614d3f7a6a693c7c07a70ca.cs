using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Serilog;

namespace ProcessCompare
{
    class Comparer
    {
        private readonly Settings _settings;
        public Comparer(Settings settings)
        {
            _settings = settings;
        }

        public void DoCompare()
        {
            DoSqlDataCompare();
        }

        private int DoSqlDataCompare()
        {
            Log.Debug("Running Red Gate Data Compare.");

            var processName = Path.Combine(_settings.RedGateDataComparePath, "SQLDataCompare.exe");
            var args =
                $"/Project:\"{Path.Combine(_settings.CompareProjectLocation, _settings.CompareProjectName)}\" /Verbose /force /Export:\"{_settings.CompareExportFolder}\"";

            var psi = new ProcessStartInfo(processName)
            {
                Arguments = args,
                UseShellExecute = false,
                CreateNoWindow = true,
                RedirectStandardOutput = true
            };

            var proc = Process.Start(psi);

            while (proc != null && !proc.StandardOutput.EndOfStream)
            {
                Log.Information("{process} {message}", "RedGateDataCompare", proc.StandardOutput.ReadLine());
            }

            proc?.WaitForExit();
            var exitCode = proc?.ExitCode ?? -1;
            proc?.Close();

            //TODO test exit code here.

            Log.Debug("Red Gate Data Compare finished. Exit code {exitCode}", exitCode);
            return exitCode;
        }

    }
}