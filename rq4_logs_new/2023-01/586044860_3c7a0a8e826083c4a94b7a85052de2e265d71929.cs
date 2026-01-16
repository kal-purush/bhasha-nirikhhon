using CommonServiceLocator;
using Engine.DataStructures;
using Engine.DataStructures.Hash;
using Engine.Interfaces;
using Engine.Interfaces.Config;
using Engine.Models.Enums;
using Engine.Strategies.Base;
using Engine.Strategies.End;
using Engine.Strategies.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Engine.Strategies.Aspiration
{
    public abstract class AspirationStrategyBase : StrategyBase
    {
        protected int AspirationDepth;
        protected int AspirationMinDepth;
        protected TranspositionTable Table;

        protected List<AspirationModel> Models;

        protected AspirationStrategyBase(short depth, IPosition position) : base(depth, position)
        {
            var service = ServiceLocator.Current.GetInstance<ITranspositionTableService>();

            var table = service.Create(depth);
            Table = table;
            Models = new List<AspirationModel>();

            var configurationProvider = ServiceLocator.Current.GetInstance<IConfigurationProvider>();
            var configuration = configurationProvider.AlgorithmConfiguration.AspirationConfiguration;
            AspirationDepth = configuration.AspirationDepth;
            AspirationMinDepth = configuration.AspirationMinDepth;

            var t = Depth - AspirationDepth * configuration.AspirationIterations[depth];

            var model = new AspirationModel { Window = SearchValue, Depth = t };
            Models.Add(model);

            for (int d = t + AspirationDepth; d <= depth; d += AspirationDepth)
            {
                var aspirationModel = new AspirationModel { Window = configuration.AspirationWindow[d], Depth = d };
                Models.Add(aspirationModel);
            }

            InitializeModels(table);
        }
        public override int Size => Table.Count;

        protected abstract void InitializeModels(TranspositionTable table);

        public override IResult GetResult()
        {
            if (Position.GetPhase() == Phase.End)
            {
                return EndGameStrategy.GetResult();
            }

            IResult result = new Result
            {
                GameResult = GameResult.Continue,
                Move = null,
                Value = 0
            };

            foreach (var model in Models)
            {
                var alpha = result.Value - model.Window;
                var beta = result.Value + model.Window;

                result = model.Strategy.GetResult(alpha, beta, model.Depth, result.Move);

                if (result.Value >= beta || result.Value <= alpha)
                {
                    result = model.Strategy.GetResult(-SearchValue, SearchValue, model.Depth, result.Move);
                }
            }

            return result;
        }

        protected override StrategyBase CreateEndGameStrategy()
        {
            return new LmrCombinedStrategy((short)Math.Min(Depth + 1, MaxEndGameDepth), Position);
        }
    }
}