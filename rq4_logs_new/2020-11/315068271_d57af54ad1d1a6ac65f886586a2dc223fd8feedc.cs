using Blazored.LocalStorage;
using DeusVultClicker.Client.State;
using System.Linq;
using System.Threading.Tasks;

namespace DeusVultClicker.Client.Services
{
    public class SaveStateService
    {
        private readonly EraService eraService;
        private readonly BuildingService buildingService;
        private readonly UpgradeService upgradeService;
        private readonly StateView stateView;
        private readonly ILocalStorageService localStorage;

        public SaveStateService(EraService eraService, BuildingService buildingService, UpgradeService upgradeService, StateView stateView, ILocalStorageService localStorage)
        {
            this.eraService = eraService;
            this.buildingService = buildingService;
            this.upgradeService = upgradeService;
            this.stateView = stateView;
            this.localStorage = localStorage;
        }
        public async Task SaveToLocalStorageAsync()
        {
            await localStorage.SetItemAsync("appstate", ToSaveState(stateView.State));
        }
        public async Task LoadFromStorageAsync()
        {
            stateView.State = ToApplicationState(await localStorage.GetItemAsync<SaveState>("appstate"));
        }
        private static SaveState ToSaveState(ApplicationState applicationState) => new SaveState
        {
            Faith = applicationState.Faith,
            Followers = applicationState.Followers,
            Money = applicationState.Money,
            CurrentEraId = applicationState.EraState.Era.Id,
            PastEras = applicationState.EraState.PastEras,
            OwnedBuildingIds = applicationState.BuildingState.OwnedBuildings.Select(i => i.Id).ToList(),
            PurchasedUpgradeIds = applicationState.UpgradeState.PurchasedUpgrades.Select(i => i.Id).ToList()
        };
        private ApplicationState ToApplicationState(SaveState saveState) => saveState == null
            ? new ApplicationState()
            : new ApplicationState
            {
                Faith = saveState.Faith,
                Money = saveState.Money,
                Followers = saveState.Followers,
                EraState = new EraState
                {
                    Era = this.eraService.GetEraAdvancementById(saveState.CurrentEraId),
                    PastEras = saveState.PastEras
                },
                BuildingState = new BuildingState
                {
                    OwnedBuildings = this.buildingService.GetBuildingsByIds(saveState.OwnedBuildingIds)
                },
                UpgradeState = new UpgradeState
                {
                    PurchasedUpgrades = this.upgradeService.GetUpgradesByIds(saveState.PurchasedUpgradeIds)
                }
            };
    }
}