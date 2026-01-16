using System.Diagnostics;

namespace MixTelematics.Utilities
{
    public class CPUProcessHelper
    {
        public static void IncreaseProcessPriorityToRealTime()
        {
            Process currentProcess = Process.GetCurrentProcess();
            currentProcess.PriorityClass = ProcessPriorityClass.RealTime;
        }
    }
}