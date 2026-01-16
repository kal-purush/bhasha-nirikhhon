using Quartz.Classes;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace Quartz.AV
{
    /// <summary>
    /// Interaction logic for ProcessRunner.xaml
    /// </summary>
    public partial class ProcessRunner : Page
    {
        public static ArrayList allProcesses;
        public static int waitTime; //ms
        public static ObservableCollection<ProcessInfo> pcsdata;
        private static double cpuWarnLvl = 0.1;
        private static double gpuWarnLvl = 0.1;
        private static double diskWarnLvl = 0.1;
        private static double netWarnLvl = 0.1;
        private static double ramWarnLvl = 1048576;
        public static ArrayList currentprocesses;

        public ProcessRunner()
        {
            InitializeComponent();
            allProcesses = GetProcessNames();
            new Thread(GetProcessExitTimes).Start();
            
        }

        //Thread 
        public static void RunThis()
        {

            
        }

        // GET PROCESSES RUNNING, AND LOG INTO TEXT FILES

        public static void CheckNewProcesses()
        {
            ArrayList currentProcesses = new ArrayList();
            while (true)
            {
                currentProcesses = GetProcessNames();
                foreach (string process in currentProcesses)
                {
                    if (!allProcesses.Contains(process))
                    {
                        allProcesses.Add(process);
                        Debug.WriteLine("New Process detected! " + process);
                        Thread PMoniter = new Thread(new ParameterizedThreadStart(MoniterProcess));
                        PMoniter.SetApartmentState(ApartmentState.STA);
                        PMoniter.Start(process);
                    }
                }
            }
        }


        public static void MoniterProcess(object arg)
        {
            TimeSpan runTime = new TimeSpan(0);
            DateTime startTime = DateTime.Now;
            DateTime lastTime = new DateTime();
            TimeSpan lastTotalProcessorTime = new TimeSpan();
            DateTime curTime;
            TimeSpan curTotalProcessorTime = new TimeSpan();
            string process = (string)arg;

            System.Diagnostics.Process[] pname = System.Diagnostics.Process.GetProcessesByName(process);
            
                LogPcsTime(process, startTime, runTime, DateTime.Now);
                allProcesses.Remove(process);
                return;
            

        }

        public static void LogPcsTime(string process, DateTime start, TimeSpan runTime, DateTime end)
        {
            Debug.WriteLine("Logging: " + process + "|" + start + "|" + runTime + "|" + end);
            string path = "..\\..\\..\\HQ\\Logs\\ProcessTimes.txt";
            // This text is added only once to the file.
            //if (!File.Exists(path))
            //{
            // Create a file to write to.
            for (var i = 0; i < 10; i++)
            {
                try
                {
                    using (StreamWriter sw = File.AppendText(path))
                    {
                        sw.WriteLine(process + "|" + start + "|" + runTime + "|" + end);
                    }
                    return;
                }
                catch (Exception e)
                {
                    Debug.WriteLine("File in use! Try " + i);
                    Thread.Sleep(waitTime / 2);
                }
            }


            //}
        }


        public static ArrayList GetProcessNames()
        {
            ArrayList currentProcesses = new ArrayList(Process.GetProcesses());
            ArrayList pcsNames = new ArrayList();
            foreach (System.Diagnostics.Process process in currentProcesses)
            {
                pcsNames.Add(process.ProcessName);
                Debug.WriteLine(process.ProcessName);
            }
            return pcsNames;
        }

        public static void GetProcessExitTimes(object arg)
        {
            string processs = (string)arg;
            System.Diagnostics.Process[] pname = System.Diagnostics.Process.GetProcessesByName(processs);
            
            foreach (Process process in pname)
            { 
            process.EnableRaisingEvents = true;
            process.Exited += (sender, e) => { Debug.WriteLine("There has been some kind of exit"); };
        }
            // var isRunning = !process.HasExited;
            return;
        }

        public static void DisplayDatagrid(string name, TimeSpan startTime)
        {
            //DatagridAvan2.
            //
        }
    }
}