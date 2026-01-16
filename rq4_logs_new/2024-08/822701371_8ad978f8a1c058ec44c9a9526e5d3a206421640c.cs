using System.IO.Pipes;

namespace Practice7_ThreadsConsole
{
    public class TaskController
    {
        private const string FILENAME = "piter1.txt";
        private List<TaskWrapper> tasks;
        private NamedPipeServerStream pipeServer;
        private int delay, itemsToLoad;

        public TaskController(NamedPipeServerStream pipeServer, int delay, int itemsToLoad)
        {
            this.pipeServer = pipeServer;
            this.delay = delay;
            this.itemsToLoad = itemsToLoad;
            tasks = new List<TaskWrapper>();
        }

        public void CreateTask()
        {
            tasks.Add(new TaskWrapper(TaskReadFile));
        }
        public void RemoveTask()
        {
            if (tasks.Count > 0)
            {
                tasks.Last().CancelTask();
                //tasks[^1].Task.Dispose();
                tasks.RemoveAt(tasks.Count - 1);
            }
            else
            {
                using (StreamWriter writer = new StreamWriter(pipeServer))
                {
                    string line = null;
                    writer.WriteLine(line);
                    writer.Flush();
                }
            }
        }

        private void TaskReadFile(CancellationToken token)
        {
            using (StreamWriter writer = new StreamWriter(pipeServer))
            {
                string[] lines = File.ReadAllLines(FILENAME);
                for (int i = 0; i < itemsToLoad && i < lines.Length; i++)
                {
                    if (token.IsCancellationRequested) break;

                    writer.WriteLine(lines[i]);
                    writer.Flush();
                    Thread.Sleep(delay);
                }
            }
        }
    }
}