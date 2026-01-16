#nullable enable
using System.Collections.Generic;
using System.Linq;

namespace Daipan.Enemy.Scripts
{
    public static class EnemySpawnCalculator
    {
        /// <summary>
        ///     Bossを含めないスポーン確率の入ったリストを受け取り、Bossを含めて返す
        /// </summary>
        public static List<float> NormalizeEnemySpawnRatioWithBoss(List<float> ratio, float bossRatio)
        {
            var beforeRatio = ratio.Sum();
            var afterRatio = (100f - bossRatio) / beforeRatio;

            var ret = ratio.Select(x => x * afterRatio).ToList();
            ret.Add(bossRatio);
            return ret;
        }
    }
}