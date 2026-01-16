using CardGame.Model.Players.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CardGame.Model.Cards.Interfaces;

namespace CardGame.Model.Engine
{
    public enum GameBoardZone
    {
        Deck = 0,
        Hand = 1,
        Board = 2,
        Graveyard = 3,
    }

    public static class GameBoardZoneExtensions
    {
        public static List<ICard> GetPlayerBoardZone(this GameBoardZone zone, IPlayer player)
        {
            switch (zone)
            {
                case GameBoardZone.Deck:
                    return player.Deck.Cards;
                case GameBoardZone.Hand:
                    return player.CardsInHand;
                case GameBoardZone.Board:
                    return player.CardsInPlay;
                case GameBoardZone.Graveyard:
                    return player.CardsInGraveyard;
                default:
                    throw new ArgumentOutOfRangeException(nameof(zone), zone, null);
            }
        }
    }
}