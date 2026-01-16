using System;
using System.Collections.Generic;
using Text_Dominion.Dominion.Enums;
using Text_Dominion.Dominion.Interface;
using Text_Dominion.Factory.Interface;
using Text_Dominion.Interface;
using Text_Dominion.Player.Interface;

namespace Text_Dominion.Dominion
{
    public class Card : ICard
    {
        private readonly ITable _table;
        private readonly IDominionFactory _factory;

        public Card(ITable table, IDominionFactory factory)
        {
            _table = table ?? throw new ArgumentNullException(nameof(table));
            _factory = factory ?? throw new ArgumentNullException(nameof(factory));
        }

        public string Title { get; set; }
        public string Description { get; set; }
        public byte Cost { get; set; }
        public List<CardType> Types { get; set; }

        public virtual void PlayCardAction(ref IPlayer player)
        {
            
        }
    }
}