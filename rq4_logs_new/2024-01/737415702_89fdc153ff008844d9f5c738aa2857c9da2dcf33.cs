using SwiftConductor.Client.Interfaces;
using SwiftConductor.Client.Models;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace SwiftConductor.Client.Worker
{
    public class Worker : IWorker
    {
        public string TaskType { get; }
        public WorkerSettings WorkerSettings { get; }

        private readonly object _workerInstance;
        private readonly MethodInfo _executeTaskMethod;

        public Worker(string taskType, WorkerSettings workerSettings, MethodInfo executeTaskMethod, object workerInstance = null)
        {
            TaskType = taskType;
            WorkerSettings = workerSettings;
            _executeTaskMethod = executeTaskMethod;
            _workerInstance = workerInstance;
        }

        public async Task<WorkerTaskResult> Run(Models.WorkerTask task, CancellationToken token)
        {

            if (token != CancellationToken.None && token.IsCancellationRequested)
                return new WorkerTaskResult() { Status = WorkerTaskResult.StatusEnum.FAILED, ReasonForIncompletion = "Token Requested Cancel" };

            var taskResult = await System.Threading.Tasks.Task.Run(() => _executeTaskMethod.Invoke(_workerInstance, new object[] { task }));
            return (WorkerTaskResult)taskResult;
        }
    }
}