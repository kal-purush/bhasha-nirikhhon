using System;
using System.Collections.Generic;
using DBT.Commons;
using DBT.Extensions;
using DBT.Network;
using DBT.Transformations;
using Terraria;
using Terraria.ID;

namespace DBT.Players
{
    public sealed partial class DBTPlayer
    {
        public void ResetOverloadEffects()
        {
            MaxOverload = 100;
            OverloadDecayRate = 20;
            OverloadGrowthRate = 1;
        }

        public void PreUpdateOverload()
        {

        }

        public void PostUpdateOverload()
        {
            if (Overload <= MaxOverload)
            {
                if (IsTransformed(TransformationDefinitionManager.Instance.LSSJ))
                {

                }
            }
        }




        public int OverloadGrowthRate { get; set; }
        public int OverloadDecayRate { get; set; }
        public int MaxOverload { get; set; }
        public int Overload { get; set; }
    }
}