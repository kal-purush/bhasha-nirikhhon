using System;

namespace WowAPI.Item
{
    public class Item
    {
        private readonly string region, locale;

        public Item(string region, string locale)
        {
            this.region = region;
            this.locale = locale;
        }

        private object CreateNewInstance(Type type, string id)
        {
            return ApiHelper.CreateNewInstance(type, new object[] { id, region, locale });
        }

        public ItemInfo ItemInfo(string itemId)
        {
            return CreateNewInstance(typeof(ItemInfo), itemId) as ItemInfo;
        }

        public ItemSet ItemSet(string setId)
        {
            return CreateNewInstance(typeof(ItemSet), setId) as ItemSet;
        }
    }
}