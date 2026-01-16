using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Numerics;
using System.Text;

namespace chinese_checkers.Core.Models
{
    public class ScoreBoard
    {
        public List<ScoreBoardEntry> ScoreBoardEntries { get; set; }

        public ScoreBoard(List<Player> players)
        {
            ScoreBoardEntries = new List<ScoreBoardEntry>();

            var ordered = players.OrderBy(x => x.Score).Reverse().ToList();
            foreach (var P in ordered)
            {
                ScoreBoardEntries.Add(new ScoreBoardEntry(P, ScoreBoardEntries.Count, ordered.IndexOf(P)));
            }
        }

        public void UpdateDestinations(List<Player> players)
        {
            var ordered = players.OrderBy(x => x.Score).Reverse().ToList();
            foreach (var P in ordered)
            {
                ScoreBoardEntries.Find(x => x.Player == P).Destination = ordered.IndexOf(P);
                
            }
        }
    }

    public class ScoreBoardEntry
    {
        public float Position { get; set; }
        public float Destination { get; set; }
        public Player Player { get; set; }

        public ScoreBoardEntry(Player player, float position, float destination)
        {
            Player = player;
            Position = position;
            Destination = destination;
        }
    }
}