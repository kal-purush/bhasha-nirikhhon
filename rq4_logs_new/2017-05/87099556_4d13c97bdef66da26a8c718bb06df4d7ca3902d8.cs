using System;

namespace BlizzardAPI.SC2.Profile
{
    public class Profile
    {
        private readonly string accountId, name, region, profileRegion, locale;

        public Profile(string accountId, string name, string region, string profileRegion, string locale)
        {
            this.accountId = accountId;
            this.name = name;
            this.region = region;
            this.locale = locale;
            this.profileRegion = profileRegion;
        }

        private object CreateNewInstance(Type type)
        {
            return ApiHelper.CreateNewInstance(type, new object[] { accountId, name, region, profileRegion, locale });
        }

        public ProfileInfo ProfileInfo()
        {
            return CreateNewInstance(typeof(ProfileInfo)) as ProfileInfo;
        }
    }
}