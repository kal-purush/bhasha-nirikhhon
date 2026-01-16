using System;
using System.Collections.Generic;
using System.Text;
using PokerEngine;
using PokerEngine.Engine;

namespace PokerConsole.AI
{
    /// <summary>
    /// An abstract base class for all of the Strategies the automated clients may take.
    /// </summary>
    public abstract class AIStrategy
    {
        /// <summary>
        /// Called by the client when an update arrives. Override this method to add logic for seeing the cards played
        /// </summary>
        /// <param name="syhcronizationData">The players updated. Must not be null</param>
        public virtual void Synchronize(IEnumerable<Player> syhcronizationData)
        {
        }

        /// <summary>
        /// Called by the client when a bet decision should be made. Derived classes must override this method to add betting logic.
        /// </summary>
        /// <param name="player">The automated player. 
        /// </param>
        /// <param name="action">The betting action which must be modified to pass the client response</param>
        public abstract void Bet(Player player, PlayerBettingAction action);
        
        /// <summary>
        /// Called by a client which needs to manually draw cards. Default implementation selects random cards. 
        /// </summary>
        /// <param name="player">The automated player</param>
        /// <param name="action">The drawing action which must be modified to pass the strategy decision</param>
        public virtual void Draw(Player player, PlayerDrawingAction action)
        {
            Random rand = new Random();
            
            int drawCount = rand.Next(3);
            
            for (int i = 0; i < drawCount; ++i)
                action.DrawnCards.Add(player.Cards[rand.Next(player.Cards.Count)]);

        }
    }
}