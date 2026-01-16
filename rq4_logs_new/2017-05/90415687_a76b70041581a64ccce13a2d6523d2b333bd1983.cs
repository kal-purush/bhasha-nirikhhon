using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;
using StardewModdingAPI;
using StardewModdingAPI.Events;
using StardewValley;

namespace DailyNewspaper
{
	public class ModEntry : Mod
	{

		public override void Entry(IModHelper helper)
		{
			// submit to events in StardewModdingAPI
			TimeEvents.DayOfMonthChanged += CheckIfNews;

		}

		private void CheckIfNews(object sender, EventArgs e)
		{
			this.Monitor.Log("newday");
			string str = Game1.shortDayNameFromDayOfSeason(Game1.dayOfMonth);
			if (str.Equals("Tue") || str.Equals("Fri") || str.Equals("Sat"))
			{

				MenuEvents.MenuChanged += Event_MenuChanged;
				this.Monitor.Log("add news");
				showMessage("Breaking News for " + UppercaseFirst(Game1.currentSeason) + " " + Game1.dayOfMonth);
			}
			else
			{
				MenuEvents.MenuChanged -= Event_MenuChanged;
				this.Monitor.Log("remove news");
			}
		}

		private void Event_MenuChanged(object sender, EventArgsClickableMenuChanged e)
		{
			string day = Game1.shortDayNameFromDayOfSeason(Game1.dayOfMonth);
			if (day.Equals("Tue") || day.Equals("Fri") || day.Equals("Sat"))
			{
				if (e.NewMenu is StardewValley.Menus.DialogueBox)
				{
					List<string> dialogues = this.Helper.Reflection.GetPrivateValue<List<string>>(e.NewMenu, "dialogues");
					if (dialogues.Count == 1 && dialogues[0] == Game1.content.LoadString("Strings\\StringsFromCSFiles:TV.cs.13120"))
					{
						this.Monitor.Log("TV MENU");
						List<Response> responseList = this.Helper.Reflection.GetPrivateValue<List<Response>>(e.NewMenu, "responses");
						Response news = new Response("News", "News Report");
						responseList.Insert(responseList.Count - 1, news);

						GameLocation.afterQuestionBehavior afterQuestion = this.Helper.Reflection.GetPrivateValue<GameLocation.afterQuestionBehavior>(Game1.currentLocation, "afterQuestion");
						this.Monitor.Log(afterQuestion.ToString());
						afterQuestion = new GameLocation.afterQuestionBehavior(this.overightChannel);

					}

				}

			}

			this.Monitor.Log(Game1.activeClickableMenu.ToString());

		}

		public static void showMessage(string msg)
		{
			var hudmsg = new HUDMessage(msg, Color.SeaGreen, 5250f, true);
			hudmsg.whatType = 2;
			Game1.addHUDMessage(hudmsg);
		}

		static string UppercaseFirst(string s)
		{
			// Check for empty string.
			if (string.IsNullOrEmpty(s))
			{
				return string.Empty;
			}
			// Return char and concat substring.
			return char.ToUpper(s[0]) + s.Substring(1);
		}

		public void overightChannel(Farmer who, string answer)
		{
			Game1.drawObjectDialogue(Game1.parseText("Your TV was Hacked!"));
		}

	}
}