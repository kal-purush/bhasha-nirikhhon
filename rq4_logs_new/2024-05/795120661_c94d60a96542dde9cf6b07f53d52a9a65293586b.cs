#nullable enable
using System.Linq;
using Daipan.Enemy.Interfaces;
using Daipan.Enemy.MonoScripts;
using Daipan.Stream.Scripts.Utility;
using UnityEngine;
using VContainer;
using VContainer.Unity;

namespace Daipan.Enemy.Scripts
{
    public sealed class EnemyBuilder
    {
        readonly IObjectResolver _container;
        readonly IEnemyFactory _enemyFactory;
        readonly IPrefabLoader<EnemyMono> _enemyMonoLoader;
        readonly EnemyAttributeParameters _attributeParameters;
        
        public EnemyBuilder(
            // IEnemyFactory enemyFactory,
            IObjectResolver container,
            IPrefabLoader<EnemyMono> enemyMonoLoader,
            EnemyAttributeParameters attributeParameters
            )
        {
            // _enemyFactory = enemyFactory;
            _container = container;
            _enemyMonoLoader = enemyMonoLoader;
            _attributeParameters = attributeParameters;
        }
        
        public EnemyMono Build(Vector3 position, Quaternion rotation)
        {
            var enemyMonoPrefab = _enemyMonoLoader.Load();
            var enemyObject = _container.Instantiate(enemyMonoPrefab, position, rotation);
            enemyObject.SetParameter(_attributeParameters.enemyParameters.First(x => x.enemyType == EnemyType.A));
            return enemyObject;
        } 
    }

}