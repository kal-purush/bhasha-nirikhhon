using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Attack.Game
{
    internal class ArtificialPlayer
    {
        private BoardMap _map;
        private GameMaster _master;
        public ArtificialPlayer(BoardMap boardMap, GameMaster gameMaster)
        {
            _map = boardMap;
            _master = gameMaster;
        }

        public void ProcessTurn()
        {
            // TODO
            // Get a list of friendly pieces
            // Get a list of empty tiles
            // Get a list of enemy pieces
            // Get a list of exposed enemy pieces

            // If a piece with range is next to exposed enemy
            // and piece can capture enemy piece
            // capture/attack that piece

            // If a piece with range is next to unexposed enemy
            // attack that piece
            // TODO consider if we want this to always happen, or priority (attack with lower ranks first?)

            // If a scout can move more than 1 tile down the Y axis do that
            // attack if possible

            // If Engineer within (4?) tiles of exposed Landmine move towards that

            // Pick random low ranked piece to move

            // Pick random hight ranked piece to move
        }
    }
}