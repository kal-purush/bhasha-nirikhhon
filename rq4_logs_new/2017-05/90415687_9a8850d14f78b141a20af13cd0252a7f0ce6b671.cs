using StardewModdingAPI;
using StardewModdingAPI.Events;
using CustomTV;
using System;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework;
using StardewValley;
using StardewValley.Objects;

namespace DailyNews
{
    public class DailyNewsMod : Mod
    {
        private int dailyNews;
        private ModConfig config;
        private Texture2D newsScreen;

        public override void Entry(IModHelper helper)
        {
            newsScreen = Helper.Content.Load<Texture2D>(@"assets\news.png");
            config = this.Helper.ReadConfig<ModConfig>();
            TimeEvents.DayOfMonthChanged += (x,y) => checkIfNews();

        }
     
        private void checkIfNews()
        {
            CustomTVMod.removeChannel("News");

            string str = Game1.shortDayNameFromDayOfSeason(Game1.dayOfMonth);
            string season = Game1.currentSeason;
            if (str.Equals("Tue") || str.Equals("Fri") || str.Equals("Sat"))
            {
                CustomTVMod.addChannel("News", "News Report", deliverNews);
                Random randomNews = new Random();
                dailyNews = randomNews.Next(0, config.newsItems.Count);
                showMessage("Breaking News for " + char.ToUpper(season[0]) + season.Substring(1) + " " + Game1.dayOfMonth);
            }
           
           
        }

        private void deliverNews(TV tv, TemporaryAnimatedSprite sprite, StardewValley.Farmer who, string answer)
        {
            
            TemporaryAnimatedSprite newsSprite = new TemporaryAnimatedSprite(newsScreen, new Rectangle(0, 0, 42, 28), 150f, 2, 999999, tv.getScreenPosition(), false, false, (float)((double)(tv.boundingBox.Bottom - 1) / 10000.0 + 9.99999974737875E-06), 0.0f, Color.White, tv.getScreenSizeModifier(), 0.0f, 0.0f, 0.0f, false);
            string text = config.newsItems[dailyNews];
            CustomTVMod.showProgram(newsSprite, text, CustomTVMod.endProgramm);
        }

        private static void showMessage(string msg)
        {
            var hudmsg = new HUDMessage(msg, Color.SeaGreen, 5250f, true);
            hudmsg.whatType = 2;
            Game1.addHUDMessage(hudmsg);
        }
    }
}