using System;
using DBT.Transformations;
using Terraria.ModLoader;

namespace DBT.Players
{
    public sealed partial class DBTPlayer : ModPlayer
    {
        public void ForAllActiveTransformations(Action<TransformationDefinition> action) => ForAllActiveTransformations(t => true, action);

        public void ForAllActiveTransformations(Predicate<TransformationDefinition> condition, Action<TransformationDefinition> action)
        {
            for (int i = 0; i < ActiveTransformations.Count; i++)
                if (condition(ActiveTransformations[i]))
                    action(ActiveTransformations[i]);
        }


        public void ForAllAcquiredTransformations(Action<PlayerTransformation> action) => ForAllAcquiredTransformations(t => true, action);

        public void ForAllAcquiredTransformations(Predicate<PlayerTransformation> condition, Action<PlayerTransformation> action)
        {
            foreach (PlayerTransformation playerTransformation in AcquiredTransformations.Values)
                if (condition(playerTransformation))
                    action(playerTransformation);
        }


        public bool ForAnyActiveTransformations(Predicate<TransformationDefinition> condition)
        {
            for (int i = 0; i < ActiveTransformations.Count; i++)
                if (condition(ActiveTransformations[i]))
                    return true;

            return false;
        }

        public bool ForAnyAcquiredTransformations(Predicate<PlayerTransformation> condition)
        {
            foreach (PlayerTransformation playerTransformation in AcquiredTransformations.Values)
                if (condition(playerTransformation))
                    return true;

            return false;
        }
    }
}