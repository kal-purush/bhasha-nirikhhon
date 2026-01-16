//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;
//using SM = OMineGuard.SettingsManager;
//using PM = OMineGuard.ProfileManager;
//using IM = OMineGuard.InformManager;
//using OCM = OMineGuard.OverclockManager;
//using MW = OMineGuard.MainWindow;
//using TCP = OMineGuard.TCPserver;
//using System.Diagnostics;
//using System.Windows.Controls;
//using System.IO;
//using System.Threading;
//using System.Windows.Media;
//using System.Windows.Documents;
//using OMineGuard.Miners;

//namespace OMineGuard
//{
    
//    public static class MinersManager
//    {
//        public static RichTextBox MinetOutput;
//        static Process Miner;
//        public static DateTime StartMiningTime;
//        public static SM.Miners? StartedMiner;
//        public static string StartedProcessName;

//        public static void StartLastMiner(object o)
//        {
//            StartLastMiner();
//        }
//        public static void StartLastMiner()
//        {
//            if (PM.Profile.StartedID == null)
//            {
//                MW.SystemMessage("Для автозапуска нужно что-нибудь запустить вручную");
//                return;
//            }
//            else
//            {
//                if (PM.GetConfig(PM.Profile.StartedID) == null)
//                {
//                    MW.SystemMessage("Автозапускаемый профиль отсутствует");
//                    return;
//                }
//                else
//                {
//                    StartMiner(PM.GetConfig(PM.Profile.StartedID));
//                }
//            }
//        }
//        public static Thread StaartProcessThread;
//        public static void StartMiner(Profile.Config Config)
//        {
//            if (Config == null)
//            {
//                MW.SystemMessage("");
//                return;
//            }
//            IM.ProcessСompleted = false;
//            IM.StartIdleWatchdog();
//            KillProcess();
//            try
//            {
//                StaartProcessThread.Abort();
//            }
//            catch { }
//            StaartProcessThread = new Thread(new ThreadStart(() =>
//            {
//                Profile.Overclock Clock = PM.GetClock(Config.ClockID);
//                if (Clock != null)
//                {
//                    if (!OCM.MSIconnecting)
//                    {
//                        MW.SystemMessage("Ожидание соединения с MSI Afterburner");
//                        while (!OCM.MSIconnecting)
//                        {
//                            Thread.Sleep(1000);
//                        }
//                        MW.SystemMessage("Соединение с MSI Afterburner установлено");
//                    }

//                    OCM.ApplyOverclock(Clock);
//                }
//                StartedMiner = Config.Miner;
//                string DT = DateTime.Now.ToString("HH.mm.ss - dd.MM.yy");
//                StartedProcessName = default;
//                string param = default;
//                string logfile = $"MinersLogs/log {DT} {Config.Name}.txt";
//                string dir = default;
//                //Клеймор
//                if (Config.Miner == SM.Miners.Claymore)
//                {
//                    dir = "Claymore's Dual Miner";
//                    string lf = "MinersLogs/buflog.txt";
//                    StartedProcessName = "EthDcrMiner64";
//                    param = $" -epool {Config.Pool}:{Config.Port} " +
//                        $"-ewal {Config.Wallet}.{PM.Profile.RigName} " +
//                        $"-logfile \"{lf}\" -wd 0 {Config.Params}";

//                    if (PM.Profile.GPUsSwitch != null)
//                    {
//                        string di = "";
//                        int n = PM.Profile.GPUsSwitch.Length;
//                        for (int i = 0; i < n; i++)
//                        {
//                            if (PM.Profile.GPUsSwitch[i])
//                            {
//                                di += "," + i;
//                            }
//                            di = di.TrimStart(',');
//                        }
//                        if (di != "")
//                        {
//                            param += " -di " + di;
//                        }
//                    }

//                    Miner = new Process();
//                    Miner.StartInfo = new ProcessStartInfo($"{dir}/{StartedProcessName}", param);
//                    Miner.StartInfo.UseShellExecute = false;
//                    Miner.StartInfo.CreateNoWindow = true;
//                    Miner.Start();

//                    Task.Run(() => ReadLog(lf, logfile));
//                }
//                //Гмайнер
//                if (Config.Miner == SM.Miners.Gminer)
//                {
//                    dir = "Gminer";
//                    StartedProcessName = "miner";
//                    string algo = "";

//                    switch (Config.Algoritm)
//                    {
//                        case "BeamHash II":
//                            algo = "beamhash";
//                            break;
//                        case "Equihash 96.5":
//                            algo = "equihash96_5";
//                            break;
//                        case "Equihash 144.5":
//                            algo = "equihash144_5";
//                            break;
//                        case "Equihash 150.5":
//                            algo = "equihash150_5";
//                            break;
//                        case "Equihash 192.7":
//                            algo = "equihash192_7";
//                            break;
//                        case "Equihash 210.9":
//                            algo = "equihash210_9";
//                            break;
//                        case "cuckARoo29":
//                            algo = "cuckaroo29";
//                            break;
//                        case "cuckAToo31":
//                            algo = "cuckatoo31";
//                            break;
//                        case "CuckooCycle":
//                            algo = "cuckoo";
//                            break;
//                    }
//                    param = $"--algo {algo} --server {Config.Pool} --port {Config.Port} " +
//                        $"--api 3333 --user {Config.Wallet}.{PM.Profile.RigName} " +
//                        $"{Config.Params} --logfile \"{logfile}\"";

//                    if (PM.Profile.GPUsSwitch != null)
//                    {
//                        string di = "";
//                        int n = PM.Profile.GPUsSwitch.Length;
//                        for (int i = 0; i < n; i++)
//                        {
//                            if (PM.Profile.GPUsSwitch[i])
//                            {
//                                di += " " + i;
//                            }
//                            di = di.TrimStart(' ');
//                        }
//                        if (di != "")
//                        {
//                            param += $" --devices {di}";
//                        }
//                    }

//                    Miner = new Process();
//                    Miner.StartInfo = new ProcessStartInfo($"{dir}/{StartedProcessName}", param);
//                    Miner.StartInfo.UseShellExecute = false;
//                    // set up output redirection
//                    Miner.StartInfo.RedirectStandardOutput = true;
//                    Miner.StartInfo.RedirectStandardError = true;
//                    Miner.EnableRaisingEvents = true;
//                    Miner.StartInfo.CreateNoWindow = true;
//                    // see below for output handler
//                    Miner.ErrorDataReceived += proc_DataReceived;
//                    Miner.OutputDataReceived += proc_DataReceived;

//                    Task.Run(() =>
//                    {
//                        Miner.Start();

//                        Miner.BeginErrorReadLine();
//                        Miner.BeginOutputReadLine();

//                        Miner.WaitForExit();
//                    });

//                }
//                //Бмайнер
//                if (Config.Miner == SM.Miners.Bminer)
//                {
//                    dir = "Bminer";
//                    StartedProcessName = "bminer";
//                    string algo = "";

//                    switch (Config.Algoritm)
//                    {
//                        case "Equihash 144.5":
//                            algo = "equihash1445";
//                            break;
//                        case "Equihash 150.5":
//                            algo = "beam";
//                            break;
//                        case "Equihash 200.9":
//                            algo = "stratum";
//                            break;
//                        case "Ethash":
//                            algo = "ethash";
//                            break;
//                        case "Tensority":
//                            algo = "tensority";
//                            break;
//                        case "CuckooCycle":
//                            algo = "aeternity";
//                            break;
//                        case "cuckARoo29":
//                            algo = "cuckaroo29d";
//                            break;
//                        case "cuckAToo31":
//                            algo = "cuckatoo31";
//                            break;
//                        case "Zhash":
//                            algo = "zhash";
//                            break;
//                    }
//                    param = $"-uri {algo}://{Config.Wallet}.{PM.Profile.RigName}@{Config.Pool}:{Config.Port} {Config.Params} -logfile \"{logfile}\" -api 127.0.0.1:3333";

//                    if (algo == "equihash1445")
//                    {
//                        param += " -pers Auto";
//                    }

//                    if (PM.Profile.GPUsSwitch != null)
//                    {
//                        string di = "";
//                        int n = PM.Profile.GPUsSwitch.Length;
//                        for (int i = 0; i < n; i++)
//                        {
//                            if (PM.Profile.GPUsSwitch[i])
//                            {
//                                di += "," + i;
//                            }
//                            di = di.TrimStart(',');
//                        }
//                        if (di != "")
//                        {
//                            param += $" -devices {di}";
//                        }
//                    }

//                    Miner = new Process();
//                    Miner.StartInfo = new ProcessStartInfo($"{dir}/{StartedProcessName}", param);
//                    Miner.StartInfo.UseShellExecute = false;
//                    // set up output redirection
//                    Miner.StartInfo.RedirectStandardOutput = true;
//                    Miner.StartInfo.RedirectStandardError = true;
//                    Miner.EnableRaisingEvents = true;
//                    Miner.StartInfo.CreateNoWindow = true;
//                    // see below for output handler
//                    Miner.ErrorDataReceived += proc_DataReceived;
//                    Miner.OutputDataReceived += proc_DataReceived;

//                    Task.Run(() =>
//                    {
//                        Miner.Start();

//                        Miner.BeginErrorReadLine();
//                        Miner.BeginOutputReadLine();

//                        Miner.WaitForExit();

//                    });

//                }
//                MW.SystemMessage($"{Config.Name} запущен");
//                IM.InformMessage($"{Config.Name} запущен");
//                MW.context.Send(ButtonSetTitleToStop, null);
//                IM.MinHashrate = Config.MinHashrate;
//                RunIndication();
//                PM.Profile.StartedProcess = StartedProcessName;
//                PM.Profile.StartedID = Config.ID;
//                PM.SaveProfile();
//                IM.StartWaching(Config.Miner);
//                IM.StartInternetWachdog();
//            }));
//            StaartProcessThread.Start();
//        }
//        public static void KillProcess(object o)
//        {
//            KillProcess();
//        }
//        public static void KillProcess()
//        {
//            bool b = false;
//            foreach (Process proc in Process.GetProcessesByName(PM.Profile.StartedProcess))
//            {
//                try
//                {
//                    proc.Kill();
//                    b = true;
//                }
//                catch { }
//            }
//            if (b) MW.SystemMessage("Процесс завершен");
//            try
//            {
//                IndicationThread.Abort();
//                TCP.OMWsendState(false, TCP.OMWstateType.Indication);
//                TCP.OMWsendInform(false, TCP.OMWinformType.Indication);
//                IM.WachingThread.Abort();
//            }
//            catch { }
//            try
//            {
//                File.Delete("MinersLogs/buflog.txt");
//            }
//            catch { }
//            MW.WachdogMSG("");
//            IM.StopLHWatchdog();
//            MW.context.Send(MW.Sethashrate, null);
//            MW.context.Send(SetIndicationColor, Brushes.Red);
//            MW.context.Send(ButtonSetTitleToStart, null);
//        }
//        private static void ButtonSetTitleToStart(object o)
//        {
//            MW.This.KillProcess.Content = "Запустить процесс";
//            MW.This.KillProcess2.Content = "Запустить процесс";
//        }
//        private static void ButtonSetTitleToStop(object o)
//        {
//            MW.This.KillProcess.Content = "Завершить процесс";
//            MW.This.KillProcess2.Content = "Завершить процесс";
//        }
//        private static Thread RestartMiningThread;
//        private static ThreadStart RestartMiningTS = new ThreadStart(() =>
//        {
//            KillProcess();
//            Thread.Sleep(10000);
//            StartLastMiner();
//            Thread.CurrentThread.Abort();
//        });
//        public static void RestartMining(string cause)
//        {
//            MW.WriteGeneralLog($"{cause}, перезапуск майнера");
//            IM.InformMessage($"{cause}, перезапуск майнера");
//            RestartMiningThread = new Thread(RestartMiningTS);
//            RestartMiningThread.Start();
//        }
//        public static void StopRMT()
//        {
//            try
//            {
//                RestartMiningThread.Abort();
//            }
//            catch { }
//        }
//        #region Indicator
//        public static bool Indication = false;
//        public static Thread IndicationThread;
//        private static void RunIndication()
//        {
//            try
//            {
//                IndicationThread.Abort();
//            }
//            catch { }
//            IndicationThread = new Thread(new ThreadStart(() =>
//             {
//                 IndicationThread = Thread.CurrentThread;
//                 if (Process.GetProcessesByName(StartedProcessName).Length == 0)
//                 {
//                     while (Process.GetProcessesByName(StartedProcessName).Length == 0)
//                     {
//                         Thread.Sleep(50);
//                     }
//                 }
//                 // начало индикации
//                 TCP.OMWsendState(true, TCP.OMWstateType.Indication);
//                 TCP.OMWsendInform(true, TCP.OMWinformType.Indication);
//                 Indication = true;
//                 while (Process.GetProcessesByName(StartedProcessName).Length != 0 && !IM.ProcessСompleted)
//                 {
//                     MW.context.Send(SetIndicationColor, Brushes.Lime);
//                     Thread.Sleep(700);
//                     MW.context.Send(SetIndicationColor, null);
//                     Thread.Sleep(300);
//                 }
//                 // завершение индикации
//                 TCP.OMWsendState(false, TCP.OMWstateType.Indication);
//                 TCP.OMWsendInform(false, TCP.OMWinformType.Indication);
//                 Indication = false;
//                 MW.context.Send(SetIndicationColor, Brushes.Red);
//                 MW.context.Send((object o) =>
//                 {
//                     MW.This.KillProcess.Content = "Запустить процесс";
//                     MW.This.KillProcess2.Content = "Запустить процесс";
//                 }, null);
//             }));
//            IndicationThread.Start();


//        }
//        private static void SetIndicationColor(object o)
//        {
//            MW.This.IndicatorEl.Fill = (Brush)o;
//            MW.This.IndicatorEl2.Fill = (Brush)o;
//        }
//        #endregion
//        #region Logfile
//        private static void proc_DataReceived(object sender, DataReceivedEventArgs e)
//        {
//            if (e.Data != "")
//            {
//                MW.context.Send(WriteToRichTextBox, e.Data);
//            }
//        }
//        private static void ReadLog(string lf, string logfile)
//        {
//            long end = 0;
//            string str;
//            if (Process.GetProcessesByName(StartedProcessName).Length == 0)
//            {
//                while (Process.GetProcessesByName(StartedProcessName).Length != 0)
//                {
//                    Thread.Sleep(50);
//                }
//            }
//            while (Process.GetProcessesByName(StartedProcessName).Length != 0)
//            {
//                try
//                {
//                    using (FileStream fstream = new FileStream(lf, FileMode.Open))
//                    {
//                        if (fstream.Length > end)
//                        {
//                            byte[] array = new byte[fstream.Length - end];
//                            fstream.Seek(end, SeekOrigin.Current);
//                            fstream.Read(array, 0, array.Length);
//                            str = System.Text.Encoding.Default.GetString(array);

//                            if (!(str.Contains("srv") ||
//                                  str.Contains("recv") ||
//                                  str.Contains("sent")))
//                            {
//                                MW.context.Send(WriteToRichTextBox, str);
//                                Task.Run(() =>
//                                {
//                                    using (StreamWriter fstr = new StreamWriter(logfile, true))
//                                    {
//                                        fstr.WriteLine(str);
//                                    }
//                                });
//                            }

//                            end = fstream.Length;
//                        }
//                    }
//                }
//                catch { }
//                Thread.Sleep(200);
//            }
//            File.Delete("MinersLogs/buflog.txt");
//        }
//        public static void WriteToRichTextBox(object t)
//        {
//            string str = (string)t;
//            if (str != null)
//            {
//                if (StartedMiner == SM.Miners.Claymore)
//                {
//                    bool[] red =
//                    {
//                    str.Contains("Cannot connect to"),
//                    str.Contains("Failed to connect")
//                };
//                    bool[] green =
//                    {
//                    str.Contains(" Connected ")
//                };
//                    if (red.Contains(true))
//                    {
//                        WriteLog(str, Brushes.Red);
//                    }
//                    else if (green.Contains(true))
//                    {
//                        WriteLog(str, Brushes.Lime);
//                    }
//                    else
//                    {
//                        WriteLog(str);
//                    }
//                }
//                if (StartedMiner == SM.Miners.Gminer)
//                {
//                    WriteLog(str);
//                }
//                if (StartedMiner == SM.Miners.Bminer)
//                {
//                    WriteLog(str);
//                }

//                if (MW.AutoScroll)
//                {
//                    MW.This.LogScroller.ScrollToEnd();
//                }
//            }
//        }
//        public static bool ShowMinerLog = false;
//        public static void WriteLog(string str, Brush brush)
//        {
//            if (ShowMinerLog)
//            {
//                TextRange tr = new TextRange(MW.This.MinerLog.Document.ContentEnd,
//                    MW.This.MinerLog.Document.ContentEnd);
//                tr.Text = str;
//                tr.ApplyPropertyValue(TextElement.ForegroundProperty, brush);
//                MW.This.MinerLog.AppendText(Environment.NewLine);

//                TCP.OMWsendState(str, TCP.OMWstateType.Logging);
//            }
//        }
//        public static void WriteLog(string str)
//        {
//            if (ShowMinerLog)
//            {
//                TextRange tr = new TextRange(MW.This.MinerLog.Document.ContentEnd,
//                    MW.This.MinerLog.Document.ContentEnd);
//                tr.Text = str;
//                tr.ApplyPropertyValue(TextElement.ForegroundProperty, Brushes.White);
//                MW.This.MinerLog.AppendText(Environment.NewLine);

//                TCP.OMWsendState(str, TCP.OMWstateType.Logging);
//            }
//        }
//        #endregion
//    }
//}