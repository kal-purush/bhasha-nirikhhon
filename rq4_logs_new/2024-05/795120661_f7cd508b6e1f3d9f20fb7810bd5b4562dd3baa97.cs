#nullable enable
using System.Collections.Generic;
using Daipan.Enemy.Scripts;
using VContainer;

namespace Daipan.LevelDesign.Enemy.Scripts
{
    public class EnemyParamDataBuilder
    {
        public EnemyParamDataBuilder(
            IContainerBuilder builder,
            EnemyParamsManager enemyParamsManager
            )
        {
            var enemyParams = new List<EnemyParamData>();
            foreach (var enemyParam in enemyParamsManager.enemyParams)
            {
                enemyParams.Add(new EnemyParamData()
                {
                    EnemyEnum = () => enemyParam.GetEnemyEnum,
                    AttackAmount = () => enemyParam.enemyAttackParam.attackAmount,
                    AttackDelayDec = () => enemyParam.enemyAttackParam.attackDelaySec,
                    AttackRange = () => enemyParam.enemyAttackParam.attackRange
                }); 
            }
            var enemyParamContainer = new EnemyParamDataContainer(enemyParams);
            builder.RegisterInstance(enemyParamContainer);
        }
    } 
}
