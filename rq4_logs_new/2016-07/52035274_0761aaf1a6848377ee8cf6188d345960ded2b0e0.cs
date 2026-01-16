using System;
using System.Collections.Generic;
using System.Linq;
using HexHelper.Hex;
using HexHelper.Hex.Interface;
using Newtonsoft.Json.Linq;

namespace HexHelper.JsonApi.HexApi
{
    public sealed class GameEndedMessage : MessageBase
    {
        public GameEndedMessage( JObject aJson, string aUser, IRepository aRepo ) : base( MessageType.GameEnded, aUser, aRepo, aJson )
        {
        }

        protected override void Parse( JObject aJson )
        {
            var theWinners = new List<string>();
            var theWinnersJson = aJson["Winners"];
            foreach( var thePlayer in theWinnersJson )
            {
                theWinners.Add( ( string ) thePlayer );
            }
            Winners = theWinners;

            var theLosers = new List<string>();
            var theLosersJson = aJson["Losers"];
            foreach( var thePlayer in theLosersJson )
            {
                theLosers.Add( ( string ) thePlayer );
            }
            Losers = theLosers;
        }

        protected override void CreateSummary()
        {
            if( Winners == null || !Winners.Any() || Losers == null || !Losers.Any() )
            {
                Summary = "Game Ended.";
            }
            else
            {
                Summary = String.Format( "Game ended: {0} wins against {1}.", Winners.First(), Losers.First() );
            }
        }

        public IEnumerable<string> Winners { get; private set; }
        public IEnumerable<string> Losers { get; private set; }
    }
}