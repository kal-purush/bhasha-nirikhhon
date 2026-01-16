using Daipan.LevelDesign.Enemy.Scripts;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Daipan.Utility.Scripts
{
    public static class Randoms
    {
        /// <summary>
        /// 割合に応じて乱数を返す
        /// </summary>
        /// <param name="ratio">各要素の割合が入ったリスト</param>
        /// <returns>抽選結果の引数</returns>
        public static int RandomByRatio(List<float> ratio)
        {
            float totalRatio = 0f;

            if (ratio.Count == 0) return 0;

            foreach (var r in ratio)
            {
                totalRatio += r;
            }

            float random = Random.value * totalRatio;

            int i;
            for (i = 0; i < ratio.Count; i++)
            {
                if (random < ratio[i])
                {
                    break;
                }
                else
                {
                    random -= ratio[i];
                }
            }

            return i;

        }
    }
}