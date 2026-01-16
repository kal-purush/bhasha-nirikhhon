using System;
using System.Linq;
using System.Windows.Forms;
using LeagueSharp;
using LeagueSharp.Common;
using SharpDX;
using Menu = LeagueSharp.Common.Menu;
using MenuItem = LeagueSharp.Common.MenuItem;

namespace WhiteFeeder
{
    class Program
    {
        private static readonly Obj_AI_Hero Player = ObjectManager.Player;
        private static Menu config;

        private static readonly bool[] CheckForBoughtItem = { false, false, false, false, false };

        private static bool point1Reached;
        private static bool point2Reached;

        private static SpellSlot ghost;

        private static bool isDead;
        private static bool saidDeadStuff;

        private static float lastChat;
        private static float lastLaugh;

        static void Main()
        {
            if (Game.Mode == GameMode.Running)
            {
                Game_OnGameLoad(new EventArgs());
            }
            else
            {
                CustomEvents.Game.OnGameLoad += Game_OnGameLoad;
            }
        }

        static void Game_OnGameLoad(EventArgs args)
        {
            Notifications.AddNotification("Loading White Feeder...", 300);

            config = new Menu("White Feeder", "unrealfeeder", true);
            config.AddItem(new MenuItem("root.shouldfeed", "Feeding Enabled").SetValue(false));
            config.AddItem(new MenuItem("root.feedmode", "Feeding Mode:").SetValue(new StringList(new[] { "Closest Enemy", "Bottom Lane", "Middle Lane", "Top Lane", "Wait at Dragon", "Wait at Baron", "Most Fed", "Highest Carrying Potential" }
            )));
            config.AddItem(new MenuItem("root.defaultto", "Default To:").SetValue(new StringList(new[] { "Bottom Lane", "Top Lane", "Middle Lane" }
            )));
            config.AddItem(new MenuItem("hehehe1", " "));
            config.AddItem(new MenuItem("root.chat", "Chat at Baron/Dragon").SetValue(false));
            config.AddItem(new MenuItem("root.chat.delay", "Baron/Dragon Chat Delay").SetValue(new Slider(2000, 0, 10000)));
            config.AddItem(new MenuItem("root.chat2", "Chat on Death").SetValue(true));
            config.AddItem(new MenuItem("hehehe2", " "));
            config.AddItem(new MenuItem("root.laugh", "Laugh").SetValue(true));
            config.AddItem(new MenuItem("root.laugh.delay", "Laugh Delay").SetValue(new Slider(500, 0, 10000)));
            config.AddItem(new MenuItem("hehehe3", " "));
            config.AddItem(new MenuItem("root.items", "Buy Speed Items").SetValue(true));
            config.AddItem(new MenuItem("hehehe4", " "));
            config.AddItem(new MenuItem("root.ghost", "Use Ghost").SetValue(false));
            config.AddItem(new MenuItem("hehehe8", " "));
            config.AddItem(new MenuItem("hehehe5", "Made by Hawk"));
            config.AddItem(new MenuItem("hehehe6", "v1.0.0.0"));
            config.AddItem(new MenuItem("hehehe7", "Site: joduska.me"));
            config.AddToMainMenu();

            ghost = Player.GetSpellSlot("summonerhaste");

            Game.OnInput += Game_OnInput;
            Game.OnUpdate += Game_OnUpdate;
            CustomEvents.Game.OnGameEnd += Game_OnGameEnd;

            Notifications.AddNotification(Player.Team == GameObjectTeam.Chaos ? "White Feeder: Team Chaos" : "White Feeder: Team Order", 300);
        }

        static void Game_OnGameEnd(EventArgs args)
        {
            Game.Say("/all Good game lads! :)");

        }

        static void Game_OnUpdate(EventArgs args)
        {
            var feedmode = config.Item("root.feedmode").GetValue<StringList>().SelectedIndex;
            var defaultto = config.Item("root.defaultto").GetValue<StringList>().SelectedIndex;

            Vector3 botTurningPoint1 = new Vector3(12124, 1726, 52);
            Vector3 botTurningPoint2 = new Vector3(13502, 3494, 51);

            Vector3 topTurningPoint1 = new Vector3(1454, 11764, 53);
            Vector3 topTurningPoint2 = new Vector3(3170, 13632, 53);

            Vector3 dragon = new Vector3(10064, 4646, -71);
            Vector3 baron = new Vector3(4964, 10380, -71);

            Vector3 chaosUniversal = new Vector3(14287f, 14383f, 172f);
            Vector3 orderUniversal = new Vector3(417f, 469f, 182f);

            string[] msgList = { "wat", "how?", "What?", "how did you manage to do that?", "mate..", "-_-",
                "why?", "lag", "laaaaag", "oh my god this lag is unreal", "rito pls 500 ping", "god bless my ping",
                "if my ping was my iq i'd be smarter than einstein", "what's up with this lag?", "is the server lagging again?",
            "i call black magic" };

            if (isDead)
            {
                if (!saidDeadStuff && config.Item("root.chat2").GetValue<bool>())
                {
                    Random r = new Random();
                    Game.Say("/all " + msgList[r.Next(0, 14)]);
                    saidDeadStuff = true;
                }
            }

            if (Player.IsDead)
            {
                isDead = true;
                point1Reached = false;
                point2Reached = false;
            }
            else
            {
                isDead = false;
                saidDeadStuff = false;

                if (Player.InFountain())
                {
                    point1Reached = false;
                    point2Reached = false;
                }

                if (Player.Distance(botTurningPoint1) <= 300 || Player.Distance(topTurningPoint1) <= 300)
                    point1Reached = true;
                if (Player.Distance(botTurningPoint2) <= 300 || Player.Distance(topTurningPoint2) <= 300)
                    point2Reached = true;
            }

            if (!config.Item("root.shouldfeed").GetValue<bool>())
            {
                return;
            }
            // ReSharper disable once CompareOfFloatsByEqualityOperator
            if ((lastLaugh == 0 || lastLaugh < Game.Time) && config.Item("root.laugh").GetValue<bool>())
            {
                lastLaugh = Game.Time + config.Item("root.laugh.delay").GetValue<Slider>().Value;
                Game.Say("/laugh");
            }

            if (ghost != SpellSlot.Unknown
                && Player.Spellbook.CanUseSpell(ghost) == SpellState.Ready
                && config.Item("root.ghost").GetValue<bool>()
                && Player.InFountain())
            {
                Player.Spellbook.CastSpell(ghost);
            }

            if (config.Item("root.items").GetValue<bool>() && Player.InShop())
            {
                if (Player.Gold >= 325
                    && !CheckForBoughtItem[0])
                {
                    Player.BuyItem(ItemId.Boots_of_Speed);
                    CheckForBoughtItem[0] = true;
                }
                if (Player.Gold >= 475
                    && CheckForBoughtItem[0]
                    && !CheckForBoughtItem[1])
                {
                    Player.BuyItem(ItemId.Boots_of_Mobility);
                    CheckForBoughtItem[1] = true;
                }
                if (Player.Gold >= 475
                    && CheckForBoughtItem[1]
                    && !CheckForBoughtItem[2])
                {
                    Player.BuyItem(ItemId.Boots_of_Mobility_Enchantment_Homeguard);
                    CheckForBoughtItem[2] = true;
                }
                if (Player.Gold >= 435
                    && CheckForBoughtItem[2]
                    && !CheckForBoughtItem[3])
                {
                    Player.BuyItem(ItemId.Amplifying_Tome);
                    CheckForBoughtItem[3] = true;
                }
                if (Player.Gold >= (850 - 435)
                    && CheckForBoughtItem[3]
                    && !CheckForBoughtItem[4])
                {
                    Player.BuyItem(ItemId.Aether_Wisp);
                    CheckForBoughtItem[4] = true;
                }
                if (Player.Gold > 1100
                    && CheckForBoughtItem[4])
                {
                    Player.BuyItem(ItemId.Zeal);
                }

            }


            switch (feedmode)
            {
                case 0:
                    if (HeroManager.Enemies.Where(x => x.IsValidTarget() && !x.IsDead).OrderBy(x => x.Distance(Player.Position)).FirstOrDefault().IsValidTarget())
                    {
                        Player.IssueOrder(GameObjectOrder.MoveTo,
                            HeroManager.Enemies.Where(x => x.IsValidTarget() && !x.IsDead).OrderBy(x => x.Distance(Player.Position)).FirstOrDefault());
                    }
                    else
                    {
                        switch (defaultto)
                        {
                            case 0:
                            {
                                if (Player.Team == GameObjectTeam.Order)
                                {
                                    if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint1);
                                    }
                                    else if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint2);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, chaosUniversal);
                                    }
                                }
                                else
                                {
                                    if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint2);
                                    }
                                    else if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint1);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, orderUniversal);
                                    }
                                }
                            }
                                break;
                            case 1:
                            {
                                if (Player.Team == GameObjectTeam.Order)
                                {
                                    if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint1);
                                    }
                                    else if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint2);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, chaosUniversal);
                                    }
                                }
                                else
                                {
                                    if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint2);
                                    }
                                    else if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint1);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, orderUniversal);
                                    }
                                }
                            }
                                break;
                            case 2:
                            {
                                Player.IssueOrder(GameObjectOrder.MoveTo,
                                    Player.Team == GameObjectTeam.Order ? chaosUniversal : orderUniversal);
                            }
                                break;
                            default:
                                Console.WriteLine(@"");
                                break;
                        }
                    }
                    break;
                case 1:
                {
                    if (Player.Team == GameObjectTeam.Order)
                    {
                        if (!point1Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint1);
                        }
                        else if (!point2Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint2);
                        }
                        else if (point2Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, chaosUniversal);
                        }
                    }
                    else
                    {
                        if (!point2Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint2);
                        }
                        else if (!point1Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint1);
                        }
                        else if (point2Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, orderUniversal);
                        }

                    }
                }
                    break;
                case 2:
                {
                    Player.IssueOrder(GameObjectOrder.MoveTo,
                        Player.Team == GameObjectTeam.Order ? chaosUniversal : orderUniversal);
                }
                    break;
                case 3:
                {
                    if (Player.Team == GameObjectTeam.Order)
                    {
                        if (!point1Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint1);
                        }
                        else if (!point2Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint2);
                        }
                        else if (point2Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, chaosUniversal);
                        }
                    }
                    else
                    {
                        if (!point2Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint2);
                        }
                        else if (!point1Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint1);
                        }
                        else if (point2Reached)
                        {
                            Player.IssueOrder(GameObjectOrder.MoveTo, orderUniversal);
                        }
                    }
                }
                    break;
                case 4:
                {
                    // ReSharper disable once CompareOfFloatsByEqualityOperator
                    if ((lastChat == 0 || lastChat < Game.Time) && config.Item("root.chat").GetValue<bool>()
                        && Player.Distance(dragon) <= 300)
                    {
                        lastChat = Game.Time + config.Item("root.chat.delay").GetValue<Slider>().Value;
                        Game.Say("/all Come to dragon!");
                    }
                    Player.IssueOrder(GameObjectOrder.MoveTo, dragon);

                }
                    break;
                case 5:
                {
                    // ReSharper disable once CompareOfFloatsByEqualityOperator
                    if ((lastChat == 0 || lastChat < Game.Time) && config.Item("root.chat").GetValue<bool>()
                        && Player.Distance(baron) <= 300)
                    {
                        lastChat = Game.Time + config.Item("root.chat.delay").GetValue<Slider>().Value;
                        Game.Say("/all Come to baron!");
                    }
                    Player.IssueOrder(GameObjectOrder.MoveTo, baron);
                }
                    break;
                case 6:
                {
                    if (HeroManager.Enemies.Where(x => x.IsValidTarget() && !x.IsDead).OrderBy(x => x.ChampionsKilled).LastOrDefault().IsValidTarget())
                    {
                        Player.IssueOrder(GameObjectOrder.MoveTo,
                            HeroManager.Enemies.Where(x => x.IsValidTarget() && !x.IsDead).OrderBy(x => x.ChampionsKilled).LastOrDefault());
                    }
                    else
                    {
                        switch (defaultto)
                        {
                            case 0:
                            {
                                if (Player.Team == GameObjectTeam.Order)
                                {
                                    if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint1);
                                    }
                                    else if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint2);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, chaosUniversal);
                                    }
                                }
                                else
                                {
                                    if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint2);
                                    }
                                    else if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint1);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, orderUniversal);
                                    }
                                }
                            }
                                break;
                            case 1:
                            {
                                if (Player.Team == GameObjectTeam.Order)
                                {
                                    if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint1);
                                    }
                                    else if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint2);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, chaosUniversal);
                                    }
                                }
                                else
                                {
                                    if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint2);
                                    }
                                    else if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint1);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, orderUniversal);
                                    }
                                }
                            }
                                break;
                            case 2:
                            {
                                Player.IssueOrder(GameObjectOrder.MoveTo,
                                    Player.Team == GameObjectTeam.Order ? chaosUniversal : orderUniversal);
                            }
                                break;
                            default:
                                Console.WriteLine(@"");
                                break;
                        }
                                
                    }
                }
                    break;
                case 7:
                {
                    if (HeroManager.Enemies.FirstOrDefault(x => x.IsValidTarget() && !x.IsDead
                                                                && (x.ChampionName == "Katarina" || x.ChampionName == "Fiora" || x.ChampionName == "Jinx" || x.ChampionName == "Vayne")).IsValidTarget())
                    {
                        Player.IssueOrder(GameObjectOrder.MoveTo,
                            HeroManager.Enemies.FirstOrDefault(x => x.IsValidTarget() && !x.IsDead
                                                                    && (x.ChampionName == "Katarina" || x.ChampionName == "Fiora" || x.ChampionName == "Jinx" || x.ChampionName == "Vayne")));
                    }
                    else
                    {
                        switch (defaultto)
                        {
                            case 0:
                            {
                                if (Player.Team == GameObjectTeam.Order)
                                {
                                    if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint1);
                                    }
                                    else if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint2);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, chaosUniversal);
                                    }
                                }
                                else
                                {
                                    if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint2);
                                    }
                                    else if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, botTurningPoint1);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, orderUniversal);
                                    }
                                }
                            }
                                break;
                            case 1:
                            {
                                if (Player.Team == GameObjectTeam.Order)
                                {
                                    if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint1);
                                    }
                                    else if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint2);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, chaosUniversal);
                                    }
                                }
                                else
                                {
                                    if (!point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint2);
                                    }
                                    else if (!point1Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, topTurningPoint1);
                                    }
                                    else if (point2Reached)
                                    {
                                        Player.IssueOrder(GameObjectOrder.MoveTo, orderUniversal);
                                    }
                                }
                            }
                                break;
                            case 2:
                            {
                                Player.IssueOrder(GameObjectOrder.MoveTo, Player.Team == GameObjectTeam.Order ? chaosUniversal : orderUniversal);
                            }
                                break;
                            default:
                                Console.WriteLine(@"");
                                break;
                        }
                    }
                }
                    break;
                default:
                    Console.WriteLine(@"");
                    break;
            }
        }

        static void Game_OnInput(GameInputEventArgs args)
        {
            if (args.Input != "/getpos")
            {
                return;
            }
            args.Process = false;
            Clipboard.SetText(Player.Position.ToString());
            Notifications.AddNotification("Copied position to clipboard.", 500);
        }

    }
}