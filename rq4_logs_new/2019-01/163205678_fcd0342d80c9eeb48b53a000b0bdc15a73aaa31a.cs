using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EKlubas.Common.Services
{
    public class RewardServices
    {
        private decimal Bonus = 1.15M;

        public int CalculateCoinReward(int markPercentage, int passMark, int baseReward)
        {
            if (markPercentage < passMark)
                return 0;

            var bonusModifier = (markPercentage - passMark) / 10;
            var reward = baseReward;

            while(bonusModifier > 0)
            {
                Bonus *= Bonus;
            }

            reward = Convert.ToInt32(Math.Round(reward * Bonus));

            return reward;
        }
    }
}