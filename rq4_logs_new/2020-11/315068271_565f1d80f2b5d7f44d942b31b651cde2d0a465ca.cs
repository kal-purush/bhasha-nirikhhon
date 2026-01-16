using System.Collections.Generic;
using System.Linq;
using DeusVultClicker.Client.Advancements;

namespace DeusVultClicker.Client.Services
{
    public class BuildingService
    {
        private readonly StateView stateView;

        public BuildingService(StateView stateView)
        {
            this.stateView = stateView;
        }

        private IEnumerable<Building> BuildingStateOwnedBuildings => this.stateView.State.BuildingState.OwnedBuildings;

        public int Reach => this.BuildingStateOwnedBuildings.Sum(b => b.Reach);
        public int AvailableSpace => this.stateView.State.EraState.Era != null ? this.stateView.State.EraState.Era.AvailableSpace - this.BuildingStateOwnedBuildings.Sum(b => b.SpaceRequirement) : 0;

        public List<Building> AvailableBuildings => Buildings
            .Where(u => u.Value.Prerequisites.All(p => this.stateView.AllAdvancements.Any(a => a.Id == p)))
            .Select(u => u.Value)
            .ToList();

        public List<Building> OwnedBuildings => this.stateView.State.BuildingState.OwnedBuildings;

        public void BuyBuilding(Building building)
        {
            if (building.SpaceRequirement > AvailableSpace)
                return;

            this.stateView.State.Money -= building.Cost;
            this.stateView.State.BuildingState.OwnedBuildings.Add(building);
        }

        public void DemolishBuilding(Building building)
        {
            this.stateView.State.BuildingState.OwnedBuildings.Remove(building);
        }

        private static readonly Dictionary<string, Building> Buildings = new Dictionary<string, Building>()
        {
            {
                "boulder",
                new Building
                {
                    Id = "boulder",
                    Title = "Boulder",
                    Description = "A large stone to yell down from.",
                    FlavorText = "Everything has to start somewhere.",
                    Cost = 0,
                    Reach = 12,
                    SpaceRequirement = 1,
                    Prerequisites = new[] { "jesus-era" },
                }
            },
            {
                "podium",
                new Building
                {
                    Id = "podium",
                    Title = "Podium",
                    Description = "Set up stage in a village.",
                    FlavorText = "Shout from above.",
                    Cost = 50,
                    Reach = 50,
                    SpaceRequirement = 1,
                    Prerequisites = new[] { "post-religion-era" },
                }
            }
        };
    }
}