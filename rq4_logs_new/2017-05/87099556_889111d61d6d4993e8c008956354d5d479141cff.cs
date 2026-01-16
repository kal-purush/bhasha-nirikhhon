using System.Collections.Generic;

namespace WowAPI.Character
{
    public class Statistics
    {
        public long LastModified { get; set; }
        public string Name { get; set; }
        public string Realm { get; set; }
        public string Battlegroup { get; set; }
        public int Class { get; set; }
        public int Race { get; set; }
        public int Gender { get; set; }
        public int Level { get; set; }
        public int AchievementPoints { get; set; }
        public string Thumbnail { get; set; }
        public string CalcClass { get; set; }
        public int Faction { get; set; }
        public StatisticsPropetries StatisticsList { get; set; }
        public int TotalHonorableKills { get; set; }

        public Statistics(string name, string realm, string region, string locale)
        {
            var statisticsData = ApiHelper.GetJsonFromUrl(
               $"https://{region}.api.battle.net/wow/character/{realm}/{name}?fields=statistics&locale={locale}&apikey={ApiHandler.ApiKey}"
           );

            if (statisticsData == null)
                return;

            LastModified = statisticsData["lastModified"];
            Name = statisticsData["name"];
            Realm = statisticsData["realm"];
            Battlegroup = statisticsData["battlegroup"];
            Class = statisticsData["class"];
            Race = statisticsData["race"];
            Gender = statisticsData["gender"];
            Level = statisticsData["level"];
            AchievementPoints = statisticsData["achievementPoints"];
            Thumbnail = statisticsData["thumbnail"];
            CalcClass = statisticsData["calcClass"];
            Faction = statisticsData["faction"];
            StatisticsList = statisticsData["statistics"].ToObject<StatisticsPropetries>();
            TotalHonorableKills = statisticsData["totalHonorableKills"];
        }
    }
}