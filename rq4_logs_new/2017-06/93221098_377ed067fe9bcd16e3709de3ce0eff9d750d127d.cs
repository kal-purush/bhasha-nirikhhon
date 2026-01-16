using Arcen.AIW2.Core;
using System;
using System.Collections.Generic;
using System.Text;
using Arcen.Universal;

namespace Arcen.AIW2.External
{
    public class AIBudgetController_Vanilla : IAIBudgetController
    {
        public ArcenEnumIndexedArray_AIBudgetType<FInt> GetSpendingRatios( WorldSide side )
        {
            ArcenEnumIndexedArray_AIBudgetType<FInt> aipToQuasiAllocate = new ArcenEnumIndexedArray_AIBudgetType<FInt>();

            FInt bottomOfStep;
            FInt topOfStep;
            ArcenEnumIndexedArray_AIBudgetType<FInt> aipRatioForStep = new ArcenEnumIndexedArray_AIBudgetType<FInt>();

            bottomOfStep = FInt.Zero;
            topOfStep = FInt.FromParts( 2, 500 );
            aipRatioForStep[AIBudgetType.Wave] = FInt.FromParts( 0, 167 );
            aipRatioForStep[AIBudgetType.Reinforcement] = FInt.FromParts( 0, 83 );
            aipRatioForStep[AIBudgetType.CPA] = FInt.FromParts( 0, 125 );
            aipRatioForStep[AIBudgetType.SpecialForces] = FInt.FromParts( 0, 500 );
            aipRatioForStep[AIBudgetType.Reconquest] = FInt.FromParts( 0, 125 );
            AllocateAIPWithinStep( aipToQuasiAllocate, aipRatioForStep, bottomOfStep, topOfStep );

            bottomOfStep = topOfStep;
            topOfStep = FInt.FromParts( 10, 000 );
            aipRatioForStep[AIBudgetType.Wave] = FInt.FromParts( 0, 83 );
            aipRatioForStep[AIBudgetType.Reinforcement] = FInt.FromParts( 0, 167 );
            aipRatioForStep[AIBudgetType.CPA] = FInt.FromParts( 0, 125 );
            aipRatioForStep[AIBudgetType.SpecialForces] = FInt.FromParts( 0, 500 );
            aipRatioForStep[AIBudgetType.Reconquest] = FInt.FromParts( 0, 125 );
            AllocateAIPWithinStep( aipToQuasiAllocate, aipRatioForStep, bottomOfStep, topOfStep );

            bottomOfStep = topOfStep;
            topOfStep = FInt.FromParts( 20, 000 );
            aipRatioForStep[AIBudgetType.Wave] = FInt.FromParts( 0, 250 );
            aipRatioForStep[AIBudgetType.Reinforcement] = FInt.FromParts( 0, 000 );
            aipRatioForStep[AIBudgetType.CPA] = FInt.FromParts( 0, 125 );
            aipRatioForStep[AIBudgetType.SpecialForces] = FInt.FromParts( 0, 500 );
            aipRatioForStep[AIBudgetType.Reconquest] = FInt.FromParts( 0, 125 );
            AllocateAIPWithinStep( aipToQuasiAllocate, aipRatioForStep, bottomOfStep, topOfStep );

            bottomOfStep = topOfStep;
            topOfStep = FInt.FromParts( 50, 000 );
            aipRatioForStep[AIBudgetType.Wave] = FInt.FromParts( 0, 125 );
            aipRatioForStep[AIBudgetType.Reinforcement] = FInt.FromParts( 0, 000 );
            aipRatioForStep[AIBudgetType.CPA] = FInt.FromParts( 0, 250 );
            aipRatioForStep[AIBudgetType.SpecialForces] = FInt.FromParts( 0, 500 );
            aipRatioForStep[AIBudgetType.Reconquest] = FInt.FromParts( 0, 125 );
            AllocateAIPWithinStep( aipToQuasiAllocate, aipRatioForStep, bottomOfStep, topOfStep );

            bottomOfStep = topOfStep;
            topOfStep = (FInt)(-1);
            aipRatioForStep[AIBudgetType.Wave] = FInt.FromParts( 0, 000 );
            aipRatioForStep[AIBudgetType.Reinforcement] = FInt.FromParts( 0, 000 );
            aipRatioForStep[AIBudgetType.CPA] = FInt.FromParts( 0, 375 );
            aipRatioForStep[AIBudgetType.SpecialForces] = FInt.FromParts( 0, 500 );
            aipRatioForStep[AIBudgetType.Reconquest] = FInt.FromParts( 0, 125 );
            AllocateAIPWithinStep( aipToQuasiAllocate, aipRatioForStep, bottomOfStep, topOfStep );

            ArcenEnumIndexedArray_AIBudgetType<FInt> result = new ArcenEnumIndexedArray_AIBudgetType<FInt>();
            FInt planetsWorthOfAIP = World_AIW2.Instance.AIProgress_Effective / ExternalConstants.Instance.Balance_BaseAIPScale;
            for ( AIBudgetType i = AIBudgetType.None; i < AIBudgetType.Length; i++ )
                result[i] = aipToQuasiAllocate[i] / planetsWorthOfAIP;
            return result;
        }

        private static void AllocateAIPWithinStep( ArcenEnumIndexedArray_AIBudgetType<FInt> aipToQuasiAllocate, ArcenEnumIndexedArray_AIBudgetType<FInt> aipRatioForStep, FInt bottomOfStep, FInt topOfStep )
        {
            FInt planetsWorthOfAIP = World_AIW2.Instance.AIProgress_Effective / ExternalConstants.Instance.Balance_BaseAIPScale;
            if ( planetsWorthOfAIP <= bottomOfStep )
                return;
            FInt aipForThisStep = planetsWorthOfAIP - bottomOfStep;
            if ( topOfStep >= 0 )
                aipForThisStep = Mat.Min( aipForThisStep, ( topOfStep - bottomOfStep ) );
            for ( AIBudgetType i = AIBudgetType.None; i < AIBudgetType.Length; i++ )
                aipToQuasiAllocate[i] += ( aipForThisStep * aipRatioForStep[i] );
        }

        public void CheckForSpendingUnlockPoints(ArcenSimContext Context)
        {
            while ( true )
            {
                int availablePoints = World_AIW2.Instance.AIProgress_Effective.IntValue - World_AIW2.Instance.SpentAIUnlockPoints;
                if ( availablePoints < World_AIW2.Instance.Setup.Difficulty.AIPNeededPerUnlock )
                    break;

                List<GameEntityTypeData> eligibleUnlocks = new List<GameEntityTypeData>();

                List<BuildMenu> menus = World_AIW2.Instance.Setup.MasterAIType.BudgetItems[AIBudgetType.Reinforcement].NormalMenusToBuyFrom;
                for ( int i = 0; i < menus.Count; i++ )
                {
                    BuildMenu menu = menus[i];
                    for ( int j = 0; j < menu.List.Count; j++ )
                    {
                        GameEntityTypeData buyableType = menu.List[j];
                        if ( buyableType.AICanUseThisWithoutUnlockingIt )
                            continue;
                        if ( buyableType.CopiedFrom != null && buyableType.CopiedFrom != buyableType )
                            continue;
                        if ( World_AIW2.Instance.CorruptedAIDesigns.Contains( buyableType ) )
                            continue;
                        if ( World_AIW2.Instance.UnlockedAIDesigns.Contains( buyableType ) )
                            continue;
                        eligibleUnlocks.Add( buyableType );
                    }
                }

                if ( eligibleUnlocks.Count <= 0 )
                    break;

                GameEntityTypeData typeToUnlock = eligibleUnlocks[Context.QualityRandom.Next( 0, eligibleUnlocks.Count )];
                World_AIW2.Instance.UnlockEntityTypeForAI( typeToUnlock, Context );
            }
        }
    }
}