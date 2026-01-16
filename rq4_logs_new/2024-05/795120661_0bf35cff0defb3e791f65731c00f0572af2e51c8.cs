#nullable enable
using UnityEngine;
using Daipan.Core.Interfaces;
using Daipan.Stream.Scripts.Utility;
using VContainer;
using VContainer.Unity;

namespace Daipan.Tower.Scripts
{
    public sealed class TowerSpawner : IStart
    {
        readonly IObjectResolver _container;
        readonly IPrefabLoader<TowerMono> _towerMonoLoader;

        [Inject]
        public TowerSpawner(
            IObjectResolver container,
            IPrefabLoader<TowerMono> towerMonoLoader)
        {
            _container = container;
            _towerMonoLoader = towerMonoLoader;
        }

        void IStart.Start()
        {
            // PlayerMonoのプレハブをロードして生成 
            var towerMonoPrefab = _towerMonoLoader.Load();
            // IObjectResolverを使ってPlayerMonoを生成すると依存関係が解決される
            var position = new Vector3(-8, 0, 0); // 左
            _container.Instantiate(towerMonoPrefab, position, Quaternion.identity);
        }
    }
}