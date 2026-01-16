using System;
using System.Security.AccessControl;
using System.IO.MemoryMappedFiles;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace DrAnalyzer.Analyzer
{
    public class Analyzer
    {
        private readonly MemoryMappedFile buff;
        private readonly Mutex mutexBuff;
        private readonly Semaphore semaphoreBuff;
        private readonly Semaphore semaphoreWaiter;

        public Analyzer(int pid)
        {
            string bufferId = string.Format("Global\\dr_analyzer_buffer_{0}", pid);
            string mutexId = string.Format("Global\\dr_analyzer_mutex_{0}", pid);
            string semaphoreId = string.Format("Global\\dr_analyzer_semaphore_{0}", pid);
            string waiterSemaphoreId = string.Format("Global\\dr_analyzer_waiter_semaphore_{0}", pid);
            
            this.buff = MemoryMappedFile.CreateNew(bufferId, 76012);

            this.mutexBuff = new Mutex(false, mutexId, out bool mutexCreated);
            if (!mutexCreated)
            {
                throw new Exception("Mutex already exists");
            }

            this.semaphoreBuff = new Semaphore(1, 1, semaphoreId, out bool semaphoreCreated);
            if (!semaphoreCreated)
            {
                throw new Exception("Buffer semaphore already exists");
            }

            this.semaphoreWaiter = new Semaphore(0, 1, waiterSemaphoreId, out bool semaphoreWaiterCreated);
            if (!semaphoreWaiterCreated)
            {
                throw new Exception("Waiter semaphore already exists");
            }

            Injector.InjectByPid(pid);
        }

        public Analyzer(string exePath)
        {
            throw new NotImplementedException();
        }
    }
}