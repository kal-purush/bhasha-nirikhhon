#nullable enable
using Daipan.Utility;
using Daipan.Utility.Scripts;

namespace Daipan.Player.Scripts
{

    public class PlayerAttackParameterLoader : IPrefabLoader<PlayerMono>
    {
        readonly PrefabLoaderFromResources<PlayerMono> _loader;
     

        public PlayerAttackParameterLoader()
        {
            _loader = new PrefabLoaderFromResources<PlayerMono>("Player");
        }
        
        public PlayerMono Load()
        {
            return _loader.Load();
        }
    }

}