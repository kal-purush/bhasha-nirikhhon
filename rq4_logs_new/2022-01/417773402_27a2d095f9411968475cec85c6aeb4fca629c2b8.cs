using System.Linq;
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace Game.Characters
{
    public class EffectsHandler : MonoBehaviour
    {
        RestrictAction _RestrictedActions;
        
        List<Effect> _EffectList = new List<Effect>();

        /// <summary>
        /// Restricted Actions of the Character
        /// </summary>
        public RestrictAction restrictedActions => _RestrictedActions;

        /// <summary>
        /// Effects casted upon the Character.
        /// </summary>
        /// <returns></returns>
        public Effect[] effects => _EffectList.ToArray();

        protected void UpdateRestrainedActions()
        {
            RestrictAction RestrainedActions = default;

            // Adds Restrict Actions to RestrainedActions
            foreach(RestrictAction r in _EffectList.Where(effect => effect.GetType() is IRestrainActionEffect).Select(effect => (effect as IRestrainActionEffect).restrictAction))//_EffectList.Where(effect => effect.GetType().IsSubclassOf(typeof(ActiveEffect))).Select(effect => (effect as ActiveEffect).restrictAction))
                RestrainedActions |= r;

            _RestrictedActions = RestrainedActions;
        }
        
        /// <summary>
        /// Adds the effects to this Character.
        /// </summary>
        /// <param name="sender">The one who sent the effect</param>
        /// <param name="effects">The one who is casted upon</param>
        public virtual void AddEffects(CharacterBase sender, CharacterBase receiver, params Effect[] effects)
        {
            Effect effect;
            Effect effectCopy;

            IEnumerable<Effect> effectsInList;
            IEnumerable<Effect> tempEffectList = _EffectList;

            foreach(IGrouping<int, Effect> effectGroup in effects.GroupBy(effect => effect.instanceId))
            {
                effect = effectGroup.First();

                effectsInList = _EffectList.Where(e => e.instanceId == effectGroup.Key);

                effectCopy = effect.isCopied ? effect : effect.CreateClone<Effect>();

                if(effect.isStackable)
                {
                    if(effectsInList.Any()) // If there is already a copy in the list, expected there is always 1 or none in the least.
                    {
                        effect = effectsInList.First();

                        effect.Stack(effectGroup.Select(e => e.isCopied ? e : e.CreateClone<Effect>()).ToArray());
                    }

                    else
                    {
                        effectCopy.Stack(effectGroup.Select(e => e.isCopied ? e : e.CreateClone<Effect>()).ToArray());
                        effectCopy.StartEffect(sender, receiver);

                        _EffectList.Add(effectCopy);
                    }
                }

                // Removes all same type in effect list and adds new one. "Refreshes" Effect
                else
                {
                    if(effectsInList.Any())
                    {
                        Effect[] effectsArray = effectsInList.ToArray();

                        for(int i = 1; i < effectsArray.Length; i++)
                            effectsArray[i].End();
                    }
                    
                    effectCopy.StartEffect(sender, receiver);

                    _EffectList.Add(effectCopy);
                }
            }
                
            foreach(IGrouping<int, Effect> effectGroup in _EffectList.GroupBy(effect => effect.instanceId))
            {
                Effect[] effectsArray = effectGroup.ToArray();

                for(int i = 1; i < effectsArray.Length; i++)
                    effectsArray[i].End();
            }
            
            UpdateRestrainedActions();
        }

        /// <summary>
        /// Removed the Effect
        /// </summary>
        /// <param name="effects"></param>
        public virtual void RemoveEffects(params Effect[] effects)
        {
            foreach(Effect effect in effects.Where(effect => _EffectList.Contains(effect)))
                _EffectList.Remove(effect);

            UpdateRestrainedActions();
        }
    }
}