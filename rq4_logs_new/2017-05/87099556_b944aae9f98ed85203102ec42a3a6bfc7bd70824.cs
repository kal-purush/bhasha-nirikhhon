using System.Collections.Generic;

namespace WowAPI.Character
{
    public class Talents
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
        public List<Talent> TalentsList { get; set; }
        public int TotalHonorableKills { get; set; }

        public Talents(string name, string realm, string region, string locale)
        {
            var talentsData = ApiHelper.GetJsonFromUrl(
              $"https://{region}.api.battle.net/wow/character/{realm}/{name}?fields=talents&locale={locale}&apikey={ApiHandler.ApiKey}"
          );

            if (talentsData == null)
                return;

            LastModified = talentsData["lastModified"];
            Name = talentsData["name"];
            Realm = talentsData["realm"];
            Battlegroup = talentsData["battlegroup"];
            Class = talentsData["class"];
            Race = talentsData["race"];
            Gender = talentsData["gender"];
            Level = talentsData["level"];
            AchievementPoints = talentsData["achievementPoints"];
            Thumbnail = talentsData["thumbnail"];
            CalcClass = talentsData["calcClass"];
            Faction = talentsData["faction"];
            TalentsList = talentsData["talents"].ToObject<List<Talent>>();
            TotalHonorableKills = talentsData["totalHonorableKills"];
        }
    }
}