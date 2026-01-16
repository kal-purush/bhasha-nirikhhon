using Blazored.LocalStorage;
using DeusVultClicker.Client.Shared.Store.Actions;
using Fluxor;
using System.Threading.Tasks;

namespace DeusVultClicker.Client.Shared.Store.Effects
{
    public class LoadSaveStateActionEffect : Effect<LoadSaveStateAction>
    {
        private readonly ILocalStorageService localStorageService;


        public LoadSaveStateActionEffect(ILocalStorageService localStorageService)
        {
            this.localStorageService = localStorageService;
        }
        [EffectMethod]
        protected async override Task HandleAsync(LoadSaveStateAction _, IDispatcher dispatcher)
        {
            var saveState = await localStorageService.GetItemAsync<SaveState>("savestate");
            if (saveState != null)
            {
                dispatcher.Dispatch(new SetBuildingStateAction(saveState.BuildingState));
                dispatcher.Dispatch(new SetEraStateAction(saveState.EraState));
                dispatcher.Dispatch(new SetUpgradeStateAction(saveState.UpgradeState));
                dispatcher.Dispatch(new SetAppStateAction(saveState.AppState));
            }
        }
    }
}