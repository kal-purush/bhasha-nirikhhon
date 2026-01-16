using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasySave.Model
{
    public interface IProcessDetector
    {
        Dictionary<string, bool> keyValuePairs { get; set; }
        public Task Task { get; set; }

        public bool CheckProcess(string processName);
    }

    internal class ProcessDetector
    {
        #region Variables
        public Dictionary<string, bool> Processes { get; set; } = new Dictionary<string, bool>();
        public Task Task { get; set; }
        #endregion

        #region Methods
        public bool CheckProcess(string processName)
        {
            if (Processes.ContainsKey(processName))
            {
                return Processes[processName];
            }
            return false;
        }
        #endregion

        #region events

        // Event to notify when a process is started
        public event Action<string> ProcessStarted;
        // Event to notify when a process is ended
        public event Action<string> ProcessEnded;
        // Event to notify when all process are ended
        public event Action AllProcessesEnded;
        // Event to notify when a process is running
        public event Action<string> ProcessRunning;
        #endregion
    }
}