using OMineGuard.Managers;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace OMineGuard.Miners
{
    class Bminer : Miner
    {
        private protected override string Directory { get; set; } = "Bminer";
        private protected override string ProcessName { get; set; } = "bminer";
        private protected override Process miner { get; set; }
        private protected override void RunThisMiner(Config Config)
        {
            Profile prof = Settings.GetProfile();
            string DT = DateTime.Now.ToString("HH.mm.ss - dd.MM.yy");
            string logfile = $"MinersLogs/log {DT} {Config.Name}.txt";

            string algo = "";

            switch (Config.Algoritm)
            {
                case "Equihash 144.5":
                    algo = "equihash1445";
                    break;
                case "Equihash 150.5":
                    algo = "beam";
                    break;
                case "Equihash 200.9":
                    algo = "stratum";
                    break;
                case "Ethash":
                    algo = "ethash";
                    break;
                case "Tensority":
                    algo = "tensority";
                    break;
                case "CuckooCycle":
                    algo = "aeternity";
                    break;
                case "cuckARoo29":
                    algo = "cuckaroo29d";
                    break;
                case "cuckAToo31":
                    algo = "cuckatoo31";
                    break;
                case "Zhash":
                    algo = "zhash";
                    break;
            }
            string param = $"-uri {algo}://{Config.Wallet}.{prof.RigName}" +
                $"@{Config.Pool}:{Config.Port} {Config.Params} " +
                $"-logfile \"{logfile}\" -api 127.0.0.1:3333";
            if (algo == "equihash1445")
            {
                param += " -pers Auto";
            }
            if (prof.GPUsSwitch != null)
            {
                string di = "";
                int n = prof.GPUsSwitch.Count;
                for (int i = 0; i < n; i++)
                {
                    if (prof.GPUsSwitch[i])
                    {
                        di += "," + i;
                    }
                    di = di.TrimStart(',');
                }
                if (di != "")
                {
                    param += $" -devices {di}";
                }
            }

            miner = new Process();
            miner.StartInfo = new ProcessStartInfo($"{Directory}/{ProcessName}", param);
            miner.StartInfo.UseShellExecute = false;
            // set up output redirection
            miner.StartInfo.RedirectStandardOutput = true;
            miner.StartInfo.RedirectStandardError = true;
            miner.EnableRaisingEvents = true;
            miner.StartInfo.CreateNoWindow = true;
            // see below for output handler
            miner.ErrorDataReceived += (s, e) => { if (e.Data != "") Task.Run(() => LogDataReceived?.Invoke(e.Data)); };
            miner.OutputDataReceived += (s, e) => { if (e.Data != "") Task.Run(() => LogDataReceived?.Invoke(e.Data)); };

            miner.Start();
            miner.BeginErrorReadLine();
            miner.BeginOutputReadLine();
        }
    }
}