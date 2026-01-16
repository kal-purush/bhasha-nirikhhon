#nullable enable
using System.Collections;
using System.Collections.Generic;
using Daipan.LevelDesign.Tower.Scripts;
using UnityEngine;

namespace Daipan.LevelDesign.Enemy.Scripts
{ 
    public sealed class TowerParamsConfig
    {
        readonly TowerPositionMono _towerPositionMono;

        public TowerParamsConfig(TowerPositionMono towerPositionMono)
        {
            _towerPositionMono = towerPositionMono;
        }

        public Vector3 GetTowerSpawnPosition()
        {
            return _towerPositionMono.towerSpawnTransform.position;
        }
    }

}