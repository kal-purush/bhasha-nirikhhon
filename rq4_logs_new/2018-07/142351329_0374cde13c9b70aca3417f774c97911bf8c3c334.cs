using GenderChess.CODE;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ChessStuff
{
    class Program
    {
        static void Main(string[] args)
        {
            
        }
    }







    /// <summary>
    /// nothing's working DO NOT RUN 
    /// </summary>
    public class ChessRules
    {
        GameDataManager gameData;
        private int currentPlayer;

        //set the board and call to the first turn
        public void StartGame()
        {
            gameData = new GameDataManager(8, 8, false,null,null);
        }

        public class Movements
        {
            Troop Troop;
            List<Coords> PossibleMovements;

            public Movements(Troop Troop, List<Coords> PossibleMovements)
            {
                this.Troop = Troop;
                this.PossibleMovements = PossibleMovements;
            }
        }
        //return list of moveable units
        public List<Troop> StartTurn(int playerId)
        {
            gameData.NextTurn();
            List<Troop> Units = gameData.GetCurrentLiving();
            List<Movements> AllMovements = new List<Movements>();
            foreach(Troop t in Units)
            {
                AllMovements.Add(new Movements(t, WhereCanMove(t)));
                if (/*היחידה יכולה לזוז*/)
                    UCanMove.Add(t);  
            }
            if (Units == null)
            {
                //סיים טור
            }
        }

        //get poss in the board and call "WhereCanMove" with the unit
        public List<Coords> WhereCanMove(Coords coords)
        {
            return WhereCanMove(gameData.GetTroopFromMap(coords));
            // beep
        }

        //get the unit and return bool matrix of the posses 
        private List<Coords> WhereCanMove(Troop troop)
        {
            var table = new int[8, 8];
            for (int y = 0; y < table.Length; y++)
            {
                for (int x = 0; x < table.Length; x++)
                {
                    table[x, y] = CanIMove(troop, new Coords();
                }
            }
            return table;
        }

        //get poss and unit and check if this unit can move there
        private bool CanIMove(Troop troop, Coords coords)
        {
            var isValid = true;

            switch (troop.Type)
            {
                case UnitTypes.King:
                    
                    break;
                case UnitTypes.Queen:
                    break;
                case UnitTypes.Bishop:
                    break;
                case UnitTypes.Pawn:
                    break;
                case UnitTypes.Knight:
                    break;
                case UnitTypes.Rook:
                    break;
            }

            var unitAtTile = gameData.UnitIn(x, y);
            if (unitAtTile.player = currentPLayer)
            {
                isValid = false;
            }
            else
            {
                if (unit.pacifist)
                {
                    isValid = false;
                }
            }
            return isValid;
        }

        public void MoveUnit(int x, int y)
        {
            Coords coords = new Coords(x, y);
            gameData.SetTroopToMap(gameData.GetTroopFromMap(coords), coords);
            //turn end
        }


    }



    //
    // var units = GetUnits();
    // ...
    // MoveUnit(currentUnit,x,y);
    // var newUnits = GetUnits();
    // //check who moved, and who no longer exists and animate accordingly
    // ...
    // StartTurn(otherPlayer);
    //
    //






}