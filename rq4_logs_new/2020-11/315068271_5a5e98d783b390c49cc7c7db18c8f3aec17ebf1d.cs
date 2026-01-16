using System.Collections.Generic;
using System.Linq;
using DeusVultClicker.Client.Shared.Store.Selectors;

namespace DeusVultClicker.Client.Buildings.Store.Selectors
{

    public class AvailableBuildingsSelector
    {
        private readonly OwnedAdvancementsSelector ownedAdvancementsSelector;

        public AvailableBuildingsSelector(OwnedAdvancementsSelector ownedAdvancementsSelector)
        {
            this.ownedAdvancementsSelector = ownedAdvancementsSelector;
        }
        public IEnumerable<Building> SelectAvailableBuildings()
        {
            var ownedAdvancmenets = this.ownedAdvancementsSelector.SelectOwnedAdvancements();
            return BuildingStorage.Buildings.Select(kv => kv.Value).Where(b => b.Prerequisites.All(p => ownedAdvancmenets.Any(o => o == p)));
        }
    }
}