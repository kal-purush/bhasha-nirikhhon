using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BrawlTCG_alpha.Logic
{
    public class Effect
    {
        // Properties
        public string Description { get; }
        public Action<object, Card, Game>? EffectAction { get; }

        // Methods
        public Effect(string description, Action<object, Card, Game>? effectAction)
        {
            Description = description;
            EffectAction = effectAction;
        }

        public void Invoke(object target, Card card, Game game)
        {
            EffectAction?.Invoke(target, card, game);
        }
    }
}