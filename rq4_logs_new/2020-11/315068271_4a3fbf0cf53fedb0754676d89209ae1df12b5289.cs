using DeusVultClicker.Client.Shared.Store.Selectors;
using System.Collections.Generic;
using System.Linq;

namespace DeusVultClicker.Client.Building.Store.Selectors
{

    public class AvailableBuildingsSelector
    {
        private readonly OwnedAdvancmenetsSelector ownedAdvancmenetsSelector;

        public AvailableBuildingsSelector(OwnedAdvancmenetsSelector ownedAdvancmenetsSelector)
        {
            this.ownedAdvancmenetsSelector = ownedAdvancmenetsSelector;
        }
        public IEnumerable<Building> SelectAvailableBuildings()
        {
            var ownedAdvancmenets = ownedAdvancmenetsSelector.SelectOwnedAdvancements();
            return BuildingStorage.Buildings.Select(kv => kv.Value).Where(b => b.Prerequisites.All(p => ownedAdvancmenets.Any(o => o == p)));
        }
    }
}