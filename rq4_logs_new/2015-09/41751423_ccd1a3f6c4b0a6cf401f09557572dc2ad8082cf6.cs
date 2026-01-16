using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace mst_boredom_remover
{
    class Game
    {
        // In C# int's are always 32-bits, if we assume 60 ticks a second we should be able to handle
        //  a game that lasts a full year long without overflowing. =D
        public int current_tick;
        public GameState game_state;
        public Dictionary<int, List<Update>> future_updates;

        public void Tick()
        {
            // Apply all updates for this tick
            foreach (var update in future_updates[current_tick])
            {
                update.Apply();
            }
            
            // We are done with all the updates for this tick
            future_updates.Remove(current_tick);

            current_tick += 1;
        }
    }
}