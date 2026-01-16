using System.Runtime.CompilerServices;
using Engine.Models.Moves;
using Engine.Sorting.Comparers;

namespace Engine.DataStructures.Moves.Collections.Advanced
{
    public class AdvancedMoveCollection : AttackCollection
    {
        protected readonly MoveList _killers;
        protected readonly MoveList _nonCaptures;
        protected readonly MoveList _notSuggested;

        public AdvancedMoveCollection(IMoveComparer comparer) : base(comparer)
        {
            _killers = new MoveList();
            _nonCaptures = new MoveList();
            _notSuggested = new MoveList();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AddKillerMove(MoveBase move)
        {
            _killers.Add(move);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AddNonCapture(MoveBase move)
        {
            _nonCaptures.Add(move);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AddNonSuggested(MoveBase move)
        {
            _notSuggested.Add(move);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override MoveBase[] Build()
        {
            var hashMovesCount = HashMoves.Count;
            var winCapturesCount = hashMovesCount + WinCaptures.Count;
            var tradesCount = winCapturesCount + Trades.Count;
            var killersCount = tradesCount + _killers.Count;
            var nonCapturesCount = killersCount + _nonCaptures.Count;
            var looseCapturesCount = nonCapturesCount + LooseCaptures.Count;
            Count = looseCapturesCount + _notSuggested.Count;

            MoveBase[] moves = new MoveBase[Count];
            if (killersCount > 0)
            {
                if (hashMovesCount > 0)
                {
                    HashMoves.CopyTo(moves, 0);
                    HashMoves.Clear();
                }

                if (WinCaptures.Count > 0)
                {
                    WinCaptures.CopyTo(moves, hashMovesCount);
                    WinCaptures.Clear();
                }

                if (Trades.Count > 0)
                {
                    Trades.CopyTo(moves, winCapturesCount);
                    Trades.Clear();
                }

                if (_killers.Count > 0)
                {
                    _killers.CopyTo(moves, tradesCount);
                    _killers.Clear();
                }
            }

            if (_nonCaptures.Count > 0)
            {
                _nonCaptures.Sort();
                _nonCaptures.CopyTo(moves, killersCount);
                _nonCaptures.Clear();
            }

            if (LooseCaptures.Count > 0)
            {
                LooseCaptures.CopyTo(moves, nonCapturesCount);
                LooseCaptures.Clear();
            }

            if (_notSuggested.Count > 0)
            {
                _notSuggested.Sort();
                _notSuggested.CopyTo(moves, looseCapturesCount);
                _notSuggested.Clear();
            }

            Count = 0;
            return moves;
        }
    }
}