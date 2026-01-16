using System.Collections.Generic;
using CardGameConsoleTestApp.Model.Cards.Interfaces;
using CardGameConsoleTestApp.Model.Engine.Interfaces;

namespace CardGameConsoleTestApp.Model.Engine
{
    public class GameBoard : IGameBoard
    {
        public GameBoard()
        {
            Player1PlayZone = new List<ICard>(GameConstants.MAX_CARDS_ON_BOARD);

            Player2PlayZone = new List<ICard>(GameConstants.MAX_CARDS_ON_BOARD);
        }

        public List<ICard> Player1PlayZone { get; set; }
        public List<ICard> Player2PlayZone { get; set; }
    }
}