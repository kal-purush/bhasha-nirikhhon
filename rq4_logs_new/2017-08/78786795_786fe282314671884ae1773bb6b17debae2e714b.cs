using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FDM90.Models;

namespace FDM90UnitTests
{
    [TestClass]
    public class TwitterDataUnitTest
    {
        [TestMethod]
        public void Test()
        {
            TwitterData tester = TwitterData.Parse("{\"NumberOfFollowers\":1375,\"Tweets\":[{\"CreatedAt\":\"2017-08-07T12:56:17Z\",\"ScreenName\":\"WalworthGarden\",\"FavoriteCount\":6,\"RetweetCount\":1,\"StatusID\":894542712900333573,\"Text\":\"Organic marrow for sale. Pop in. https://t.co/NLgcJbsaW1\",\"Retweeted\":true,\"Favorited\":true,\"RetweetedUsers\":[{\"NumberOfFollowers\":304}]},{\"CreatedAt\":\"2017-08-07T11:53:24Z\",\"ScreenName\":\"WalworthGarden\",\"FavoriteCount\":5,\"RetweetCount\":1,\"StatusID\":894526890169552896,\"Text\":\"Fruit from the Quince tree 👌🏻 https://t.co/CM4VWVA4es\",\"Retweeted\":true,\"Favorited\":true,\"RetweetedUsers\":[{\"NumberOfFollowers\":304}]},{\"CreatedAt\":\"2017-08-06T10:00:35Z\",\"ScreenName\":\"WalworthGarden\",\"FavoriteCount\":2,\"RetweetCount\":0,\"StatusID\":894136108715565056,\"Text\":\"Bright and delicate Ceratostigma plumbaginoides https://t.co/qecogAqRH9\",\"Retweeted\":false,\"Favorited\":true,\"RetweetedUsers\":[]},{\"CreatedAt\":\"2017-08-05T10:00:44Z\",\"ScreenName\":\"WalworthGarden\",\"FavoriteCount\":4,\"RetweetCount\":1,\"StatusID\":893773760641282048,\"Text\":\"Don’t forget we’re open 7days a week, so pop by for some plants and a fresh juice made from 100% fruit and veg! 👌🏻🍏 https://t.co/7gh4lzyd3Z\",\"Retweeted\":true,\"Favorited\":true,\"RetweetedUsers\":[{\"NumberOfFollowers\":82}]},{\"CreatedAt\":\"2017-08-04T17:01:06Z\",\"ScreenName\":\"WalworthGarden\",\"FavoriteCount\":3,\"RetweetCount\":1,\"StatusID\":893517160441622529,\"Text\":\"Train with us &amp; gain a level 2 diploma in #Horticulture. Contact now to start this September 5th! https://t.co/ska8rtpEZA\",\"Retweeted\":true,\"Favorited\":true,\"RetweetedUsers\":[{\"NumberOfFollowers\":32}]},{\"CreatedAt\":\"2017-08-04T15:01:42Z\",\"ScreenName\":\"WalworthGarden\",\"FavoriteCount\":3,\"RetweetCount\":0,\"StatusID\":893487112804335617,\"Text\":\"An eye catching display of flowers from the ginger #PlantofTheWeek https://t.co/b0fsZNVxhi\",\"Retweeted\":false,\"Favorited\":true,\"RetweetedUsers\":[]}]}", new TwitterData());
        }
    }
}