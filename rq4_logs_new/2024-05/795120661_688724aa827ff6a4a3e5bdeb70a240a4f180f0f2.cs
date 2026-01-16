#nullable enable
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using VContainer;


namespace Daipan.LevelDesign.Enemy.Scripts
{
    public class EnemyParamsServer
    {
        readonly EnemyParams _enemyParams = null;
        readonly EnemyPosition _enemyPosition = null;

        [Inject]
        EnemyParamsServer (EnemyParams enemyParams, EnemyPosition enemyPosition)
        {
            _enemyParams = enemyParams;
            _enemyPosition = enemyPosition;
        }

        public Vector3 GetSpawnedPosition()
        {
            var positions = _enemyPosition.enemySpawnedPoints.Select(tra => tra.position);
            return positions.First();
        }

        public Vector3 GetDespawnedPosition()
        {
            return _enemyPosition.enemyDespawnedPoint.position;
        }
    }
}