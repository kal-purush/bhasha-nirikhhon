using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SpurRoguelike.PlayerBot
{
    public interface IPriorityQueue<TKey>
    {
        void Add(TKey key, int value);
        void Update(TKey key, int value);
        bool TryGetValue(TKey key, out int value);
        Tuple<TKey, int> ExtractMinKey();
    }
}