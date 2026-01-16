using UnityEngine;
using System.Collections.Generic;
using System;

namespace CRAG.AchievementSystem
{
    public class AchievementManager : MonoBehaviour
    {
        public static AchievementManager instance;

        public event EventHandler<AchievementEventArgs> achievementUnlockedAction = delegate { };

        private Dictionary<Achievements, AchievementEventArgs> registeredAchievements =
            new Dictionary<Achievements, AchievementEventArgs>(3){
                {Achievements.CollectAllCoins, new AchievementEventArgs("Золотоискатель") },
                {Achievements.RideTheComet,    new AchievementEventArgs("Оседлать комету")},
                {Achievements.TakeInPluton,    new AchievementEventArgs("Новые горизонты")},
            };

        void Awake()
        {
            if (instance == null)
                instance = this;
            else if (instance != this)
                Destroy(gameObject);
        }

        public void UnlockAchievement(Achievements achiev)
        {
            try
            {
                if (!registeredAchievements[achiev].register)
                {
                    registeredAchievements[achiev].register = true;
                    achievementUnlockedAction(this, registeredAchievements[achiev]);
                }
            }
            catch(KeyNotFoundException e)
            {
                Debug.LogError(e.Message);
            }
        }
    }
}
