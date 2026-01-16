#nullable enable
using Daipan.Comment.MonoScripts;
using Daipan.Stream.Scripts.Utility;
using Daipan.Stream.Scripts.Utility.Scripts;

namespace Daipan.Tower.Scripts
{
    public sealed class TowerPrefabLoader : IPrefabLoader<TowerMono>
    {
        readonly PrefabLoaderFromResources<TowerMono> _loader;

        public TowerPrefabLoader()
        {
            _loader = new PrefabLoaderFromResources<TowerMono>("Tower");
        }

        public TowerMono Load()
        {
            return _loader.Load();
        }
    }
}