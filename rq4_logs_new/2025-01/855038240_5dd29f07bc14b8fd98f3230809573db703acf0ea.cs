using Domain.Enums.TournamentsInRegionEnums;

namespace Application.DatabaseMappingReferences.TournamentsInRegions
{
    public class TournamentsInRegion
    {
        public IReadOnlyDictionary<TournamentsInRegionEnum, IReadOnlyList<string>> LeagueNameByRegion { get; } = new Dictionary<TournamentsInRegionEnum, IReadOnlyList<string>>
        {
            [TournamentsInRegionEnum.Europe] = new List<string>
            {
                "Europe League Championship Series",
                "LoL EMEA Championship"
            }.AsReadOnly(),

            [TournamentsInRegionEnum.America] = new List<string>
            {
            "League of Legends Championship of The Americas North",
            "League of Legends Championship of The Americas South",
            "League of Legends Championship Series",
            "North America League Championship Series"
            }.AsReadOnly()
        };


        



    }
}