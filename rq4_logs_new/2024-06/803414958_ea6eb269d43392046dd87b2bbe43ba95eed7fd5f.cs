using Godot;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Attack.Game
{
    enum TurnAction
    {
        Invalid,
        Select,
        Move,
        Attack
    }

    internal class Turn
    {
        public Tile SelectedTile { get; set; }
        public Tile DestinationTile { get; set; }
        public Tile AttackedTile { get; set; }

        Dictionary<Vector2I, Tile> ValidMoves = new Dictionary<Vector2I, Tile>();
        Dictionary<Vector2I, Tile> ValidAttacks = new Dictionary<Vector2I, Tile>();

        Team TeamPlaying { get; set; }

        internal Turn (Team currentTeam)
        {
            TeamPlaying = currentTeam;
        }

        public TurnAction ProcessLeftClick(Tile tile, Vector2I cell)
        {
            if (SelectedTile == null)
            {
                if (tile.Piece == null)
                    return TurnAction.Invalid;

                if (tile.Piece.Team != Team.Blue)
                    return TurnAction.Invalid;

                if (tile.Piece.Range < 1)
                    return TurnAction.Invalid;

                Log.Debug("Tile selected");

                SelectedTile = tile;
                return TurnAction.Select;
            } 
            else if (SelectedTile != null && tile.Piece?.Team == Team.Blue)
            {
                // We've still got a valid selection here
                if (tile.Piece.Range < 1)
                    return TurnAction.Invalid;

                Log.Debug("Tile re-selected");

                SelectedTile = tile;
                return TurnAction.Select;
            }


            if (DestinationTile == null && ValidMoves.ContainsKey(cell))
            {
                var currentPostion = SelectedTile.Position;

                var differenceX = Math.Max(currentPostion.X, cell.X) - Math.Min(currentPostion.X, cell.X);
                var differenceY = Math.Max(currentPostion.Y, cell.Y) - Math.Min(currentPostion.Y, cell.Y);

                // Can't move diagonally
                if (differenceX > 0 && differenceY > 0)
                    return TurnAction.Invalid;

                if (differenceX > SelectedTile.Piece.Range ||  differenceY > SelectedTile.Piece.Range)
                    return TurnAction.Invalid;

                DestinationTile = tile;

                DestinationTile.AddPiece(SelectedTile.Piece);
                SelectedTile.RemovePiece();

                return TurnAction.Move;
            }

            // TODO attack

            if (DestinationTile != null)
                return TurnAction.Move;
            else if (SelectedTile != null)
                return TurnAction.Select;

            return TurnAction.Invalid;
        }

        public void ProcessRightClick()
        {
            // We can't allow attack actions to be reverted
            // would give the player free intel
            if (AttackedTile != null)
                return;

            if (SelectedTile == null)
                return;

            if (DestinationTile != null)
            {
                Log.Debug("Clearing move");

                SelectedTile.AddPiece(DestinationTile.Piece);
                DestinationTile.RemovePiece();
            }

            Log.Debug("Clearing selected tile");

            SelectedTile = null;
            DestinationTile = null;

            ValidMoves.Clear();
            ValidAttacks.Clear();
        }

        public void ClearMoves() =>
            ValidMoves.Clear();

        public void ClearAttacks() =>
            ValidAttacks.Clear();
        public void AddMove(Vector2I position, Tile tile)
        {
            if (ValidMoves.ContainsKey(position))
                return;

            ValidMoves.Add(position, tile);
        }
            

        public void AddAttack(Vector2I position, Tile tile)
        {
            if (ValidAttacks.ContainsKey(position))
                return;

            ValidAttacks.Add(position, tile);
        }
    }
}