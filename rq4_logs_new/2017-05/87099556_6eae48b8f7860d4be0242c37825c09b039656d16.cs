using System.Collections.Generic;

namespace BlizzardAPI.SC2.DataResources
{
    public class Achievements
    {
        public List<Achievement> AchievementsList { get; set; }
        public List<Category> Categories { get; set; }

        public Achievements(string region, string locale)
        {
            var achievementsData = ApiHelper.GetJsonFromUrl(
               $"https://{region}.api.battle.net/sc2/data/achievements?locale={locale}&apikey={ApiHandler.ApiKey}"
           );

            if (achievementsData == null)
                return;

            AchievementsList = achievementsData["achievements"].ToObject<List<Achievement>>();
            Categories = achievementsData["categories"].ToObject<List<Category>>();
        }
    }
}