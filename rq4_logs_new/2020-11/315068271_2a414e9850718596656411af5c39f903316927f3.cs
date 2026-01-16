using DeusVultClicker.Client.Shared.Store.Actions;
using Fluxor;
using System.Threading.Tasks;

namespace DeusVultClicker.Client.Shared.Store.Effects
{
    public class StartNewTimerActionEffect : Effect<StartNewTimerAction>
    {
        private readonly TimerService timerService;

        public StartNewTimerActionEffect(TimerService timerService)
        {
            this.timerService = timerService;
        }

        [EffectMethod]
        protected override Task HandleAsync(StartNewTimerAction action, IDispatcher dispatcher)
        {
            timerService.StartNew(action.IntervalInMs, dispatcher);
            return Task.CompletedTask;
        }
    }
}