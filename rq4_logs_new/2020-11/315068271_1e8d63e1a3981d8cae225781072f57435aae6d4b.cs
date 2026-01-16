using System.Collections.Generic;

namespace DeusVultClicker.Client.Building
{
    public static class BuildingStorage
    {
        public static readonly Dictionary<string, Building> Buildings = new Dictionary<string, Building>
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