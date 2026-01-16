using Engine.DataStructures;
using Engine.DataStructures.Moves;
using Engine.Models.Moves;
using Engine.Sorting.Sorters;
using System.Runtime.CompilerServices;

namespace Engine.Strategies.Models
{
    public abstract  class SortContext
    {
        public bool HasPv;
        public bool IsPvCapture;
        public short Pv;
        public MoveSorter MoveSorter;
        public byte[] Pieces;
        public SquareList[] Squares;

        protected SortContext()
        {
            Squares = new SquareList[6];
            for (int i = 0; i < Squares.Length; i++)
            {
                Squares[i] = new SquareList();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(MoveSorter sorter, MoveBase pv)
        {
            MoveSorter = sorter;
            MoveSorter.SetKillers();

            if(pv!= null)
            {
                HasPv = true;
                Pv = pv.Key;
                IsPvCapture = pv.IsAttack;
            }
            else
            {
                HasPv = false;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(MoveSorter sorter)
        {
            MoveSorter = sorter;
            MoveSorter.SetKillers();
            HasPv = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProcessHashMove(MoveBase move)
        {
            MoveSorter.ProcessHashMove(move);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProcessKillerMove(MoveBase move)
        {
            MoveSorter.ProcessKillerMove(move);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProcessCastleMove(MoveBase move)
        {
            MoveSorter.ProcessCastleMove(move);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public abstract void ProcessPromotionMove(MoveBase move);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ProcessCaptureMove(AttackBase move)
        {
            MoveSorter.ProcessCaptureMove(move);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public abstract void ProcessMove(MoveBase move);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsKiller(short key)
        {
            return MoveSorter.IsKiller(key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal MoveList GetMoves()
        {
            return MoveSorter.GetMoves();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void InitializeSort()
        {
            MoveSorter.InitializeSort();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void FinalizeSort()
        {
            MoveSorter.FinalizeSort();
        }
    }

    public abstract class WhiteSortContext : SortContext
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ProcessPromotionMove(MoveBase move)
        {
            MoveSorter.ProcessWhitePromotionMove(move);
        }
    }
    public class WhiteOpeningSortContext : WhiteSortContext
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ProcessMove(MoveBase move)
        {
            MoveSorter.ProcessWhiteOpeningMove(move);
        }
    }
    public class WhiteMiddleSortContext : WhiteSortContext
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ProcessMove(MoveBase move)
        {
            MoveSorter.ProcessWhiteMiddleMove(move);
        }
    }
    public class WhiteEndSortContext : WhiteSortContext
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ProcessMove(MoveBase move)
        {
            MoveSorter.ProcessWhiteEndMove(move);
        }
    }

    public abstract class BlackSortContext : SortContext 
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ProcessPromotionMove(MoveBase move)
        {
            MoveSorter.ProcessBlackPromotionMove(move);
        }
    }
    public class BlackOpeningSortContext : BlackSortContext
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ProcessMove(MoveBase move)
        {
            MoveSorter.ProcessBlackOpeningMove(move);
        }
    }
    public class BlackMiddleSortContext : BlackSortContext
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ProcessMove(MoveBase move)
        {
            MoveSorter.ProcessBlackMiddleMove(move);
        }
    }
    public class BlackEndSortContext : BlackSortContext
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override void ProcessMove(MoveBase move)
        {
            MoveSorter.ProcessBlackEndMove(move);
        }
    }
}