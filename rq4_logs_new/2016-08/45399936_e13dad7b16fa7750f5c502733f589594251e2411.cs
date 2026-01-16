using System.ServiceProcess;

namespace ChickenOrEgg
{
    public class StartableService : ServiceBase
    {
        public void Start(string[] args)
        {
            OnStart(args);
        }
    }
}