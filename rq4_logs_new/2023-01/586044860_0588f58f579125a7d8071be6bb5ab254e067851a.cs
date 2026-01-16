using Engine.DataStructures.Hash;
using Engine.Interfaces;
using Engine.Models.Moves;
using Engine.Sorting.Comparers;
using Engine.Strategies.Lmr;
using Engine.Strategies.Models;

namespace Engine.Strategies.End
{
    public class LmrEndGameStrategy : LmrStrategyBase
    {
        public LmrEndGameStrategy(short depth, IPosition position, TranspositionTable table = null) 
            : base(depth, position, table)
        {
            InitializeSorters(depth, position, MoveSorterProvider.GetInitial(position, new HistoryComparer()));
        }

        public override IResult GetResult()
        {
            return GetResult(-SearchValue, SearchValue, Depth);
        }

        public override int Search(int alpha, int beta, int depth)
        {
            if (depth <= 0) return Evaluate(alpha, beta);

            if (CheckEndGameDraw()) return 0;

            MoveBase pv = null;
            bool shouldUpdate = false;
            bool isInTable = false;

            if (Table.TryGet(Position.GetKey(), out var entry))
            {
                isInTable = true;
                var entryDepth = entry.Depth;

                if (entryDepth >= depth)
                {
                    if (entry.Value > alpha)
                    {
                        alpha = entry.Value;
                    }
                    if (alpha >= beta)
                        return entry.Value;
                }
                else
                {
                    shouldUpdate = true;
                }

                pv = GetPv(entry.PvMove);
            }

            SearchContext context = GetCurrentContext(alpha, depth, pv);

            if (context.IsEndGame)
            {
                return context.Value;
            }

            if (context.IsFutility)
            {
                FutilitySearchInternal(alpha, beta, depth, context);
                if (context.IsEndGame) return Position.GetValue();
            }
            else
            {
                SearchInternal(alpha, beta, depth, context);
            }

            if (isInTable && !shouldUpdate) return context.Value;

            return StoreValue((byte)depth, (short)context.Value, context.BestMove.Key);
        }

        protected override bool[] InitializeReducableDepthTable()
        {
            var result = new bool[2 * Depth];
            for (int depth = 0; depth < result.Length; depth++)
            {
                result[depth] = depth > 3;
            }

            return result;
        }

        protected override bool[] InitializeReducableMoveTable()
        {
            var result = new bool[128];
            for (int move = 0; move < result.Length; move++)
            {
                result[move] = move > 3;
            }

            return result;
        }

        protected override byte[][] InitializeReductionTable()
        {
            var result = new byte[2 * Depth][];
            for (int depth = 0; depth < result.Length; depth++)
            {
                result[depth] = new byte[128];
                for (int move = 0; move < result[depth].Length; move++)
                {
                    if (depth > 3 && move > 3)
                    {
                        result[depth][move] = (byte)(depth - 2);
                    }
                    else
                    {
                        result[depth][move] = (byte)(depth - 1);
                    }
                }
            }

            return result;
        }
    }
}