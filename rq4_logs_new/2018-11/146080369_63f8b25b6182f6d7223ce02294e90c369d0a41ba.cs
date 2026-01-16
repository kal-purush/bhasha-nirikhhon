using System.Threading.Tasks;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Sdk;
using Lykke.Service.Bitcoin.Api.Services.Wallet;

namespace Lykke.Service.Bitcoin.Api.Lifetime
{
    public class StartupManager:IStartupManager
    {
        private readonly BlockHeightSettings _blockHeightSettings;
        private readonly ILog _log;

        public StartupManager(BlockHeightSettings blockHeightSettings, ILogFactory logFactory)
        {
            _blockHeightSettings = blockHeightSettings;
            _log = logFactory.CreateLog(this);
        }

        public Task StartAsync()
        {
            _log.Info("App started", context: _blockHeightSettings);
            return Task.CompletedTask;
        }
    }
}