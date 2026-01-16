using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Threading;
using EloBuddy;
using EloBuddy.SDK;
using EloBuddy.SDK.Enumerations;
using EloBuddy.SDK.Events;
using EloBuddy.SDK.Menu;
using EloBuddy.SDK.Menu.Values;
using EloBuddy.SDK.Rendering;
using SharpDX;

namespace wzAmumu
{
    class Program
    {
        private static Menu menu, comboMenu, harassMenu, farmMenu, autowMenu, drawingsMenu;

        private static readonly Dictionary<SpellSlot, Spell.SpellBase> spells = new Dictionary<SpellSlot, Spell.SpellBase>
        {
            {SpellSlot.Q, new Spell.Skillshot(SpellSlot.Q, 1080, SkillShotType.Linear, 250, 2000, 90)},
            {SpellSlot.W, new Spell.Active(SpellSlot.W, 300)},
            {SpellSlot.E, new Spell.Active(SpellSlot.E, 330)},
            {SpellSlot.R, new Spell.Active(SpellSlot.R, 550)}
        };

        private static readonly string[] SmiteNames = { "s5_summonersmiteplayerganker", "itemsmiteaoe", "s5_summonersmitequick", "s5_summonersmiteduel", "summonersmite" };
        private static Spell.Targeted Smite;

        //Rewrite menu: QWER.

        static void Main(string[] args)
        {
            Loading.OnLoadingComplete += Loading_OnLoadingComplete;
        }

        static void Loading_OnLoadingComplete(EventArgs args)
        {
            if (Player.Instance.Hero != Champion.Amumu)
                return;

            #region Init Smite
            //FIX THIS SHIT OMFG.
            if (SmiteNames.Contains(ObjectManager.Player.Spellbook.GetSpell(SpellSlot.Summoner1).Name))
            {
                Smite = new Spell.Targeted(SpellSlot.Summoner1, 570);
            }
            if (SmiteNames.Contains(ObjectManager.Player.Spellbook.GetSpell(SpellSlot.Summoner2).Name))
            {
                Smite = new Spell.Targeted(SpellSlot.Summoner2, 570);
            }
            #endregion

            Bootstrap.Init(null);

            #region Main Menu
            menu = MainMenu.AddMenu("wzAmumu", "wzamumu");
            menu.AddGroupLabel("wzAmumu");
            menu.AddLabel("This is my little retarded baby addon, so no hate plz.");
            #endregion

            #region Combo Menu
            comboMenu = menu.AddSubMenu("Combo", "combomenu");
            comboMenu.AddGroupLabel("Combo");

            comboMenu.Add("combouseq", new CheckBox("Use Q"));
            comboMenu.Add("combousesmiteq", new CheckBox("Use Smite+Q Combo", false));
            comboMenu.AddSeparator();
            comboMenu.Add("combousee", new CheckBox("Use E"));
            comboMenu.AddSeparator();
            comboMenu.Add("combouser", new CheckBox("Auto R"));
            comboMenu.Add("comboautor", new Slider("Targets in R range", 3, 2, 5));
            #endregion

            #region Harass Menu
            harassMenu = menu.AddSubMenu("Harass", "harassmenu");
            harassMenu.AddGroupLabel("Harass");

            harassMenu.Add("harassuseq", new CheckBox("Use Q", false));
            harassMenu.AddSeparator();
            harassMenu.Add("harassusee", new CheckBox("Use E"));
            #endregion

            #region Farm Menu
            farmMenu = menu.AddSubMenu("Farm", "farmmenu");
            farmMenu.AddGroupLabel("Lane Clear");

            farmMenu.Add("laneclearuseq", new CheckBox("Use Q", false));
            farmMenu.AddSeparator();
            farmMenu.Add("laneclearusee", new CheckBox("Use E"));
            farmMenu.Add("laneclearcounte", new Slider("Minions in E range to use E", 3, 1, 6));
            farmMenu.AddSeparator();

            farmMenu.AddGroupLabel("Jungle Clear");
            farmMenu.Add("jungleclearuseq", new CheckBox("Use Q"));
            farmMenu.AddSeparator();
            farmMenu.Add("jungleclearusee", new CheckBox("Use E"));
            #endregion

            #region Auto W Menu
            autowMenu = menu.AddSubMenu("Auto W", "autowmenu");
            autowMenu.AddGroupLabel("Auto W Control");

            autowMenu.Add("autousew", new CheckBox("Auto W"));
            autowMenu.Add("autowmana", new Slider("Use W until Mana %", 10));
            #endregion

            #region Drawings Menu
            drawingsMenu = menu.AddSubMenu("Drawings", "drawingsmenu");
            drawingsMenu.AddGroupLabel("Drawings");

            drawingsMenu.Add("drawq", new CheckBox("Draw Q"));
            drawingsMenu.Add("draww", new CheckBox("Draw W", false));
            drawingsMenu.Add("drawe", new CheckBox("Draw E", false));
            drawingsMenu.Add("drawr", new CheckBox("Draw R"));
            drawingsMenu.AddSeparator();
            drawingsMenu.Add("drawready", new CheckBox("Only draw when ready"));
            #endregion

            Game.OnUpdate += Game_OnUpdate;
            Drawing.OnDraw += Drawing_OnDraw;
        }

        static void Game_OnUpdate(EventArgs args)
        {
            AutoR();

            switch (Orbwalker.ActiveModesFlags)
            {
                case Orbwalker.ActiveModes.Combo:
                    Combo();
                    break;
                case Orbwalker.ActiveModes.Harass:
                    Harass();
                    break;
                default:
                    //Turn off W when recalling.
                    if (Player.Instance.IsRecalling && spells[SpellSlot.W].Handle.ToggleState == 2)
                        spells[SpellSlot.W].Cast();
                    break;
            }

            //Fix for: if someone has binded Laneclear and jungleclear on the same key.
            if (Orbwalker.ActiveModesFlags.HasFlag(Orbwalker.ActiveModes.LaneClear))
                LaneClear();
            if (Orbwalker.ActiveModesFlags.HasFlag(Orbwalker.ActiveModes.JungleClear))
                JungleClear();
        }

        private static void Drawing_OnDraw(EventArgs args)
        {
            List<KeyValuePair<string, SpellSlot>> drawSpellsList = new List<KeyValuePair<string, SpellSlot>>
            {
                new KeyValuePair<string, SpellSlot>("q", SpellSlot.Q),
                new KeyValuePair<string, SpellSlot>("w", SpellSlot.W),
                new KeyValuePair<string, SpellSlot>("e", SpellSlot.E),
                new KeyValuePair<string, SpellSlot>("r", SpellSlot.R)
            };

            //Resharper can make this look even harder than it already is with Linq but I'm just gonna let it be this way.
            foreach (KeyValuePair<string, SpellSlot> pair in drawSpellsList)
            {
                if (drawingsMenu["draw" + pair.Key].Cast<CheckBox>().CurrentValue)
                {
                    if (drawingsMenu["drawready"].Cast<CheckBox>().CurrentValue && spells[pair.Value].IsReady() || !drawingsMenu["drawready"].Cast<CheckBox>().CurrentValue)
                    {
                        Circle.Draw(Color.DarkGray, spells[pair.Value].Range, Player.Instance.Position);
                    }
                }
            }
        }

        private static void Combo()
        {
            if (comboMenu["combouseq"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.Q].IsReady())
            {
                AIHeroClient target = TargetSelector.GetTarget(spells[SpellSlot.Q].Range, DamageType.Magical);

                if (target != null && !Player.Instance.IsInAutoAttackRange(target))
                {
                    if (!spells[SpellSlot.Q].Cast(target) && comboMenu["combousesmiteq"].Cast<CheckBox>().CurrentValue)
                    {
                        PredictionResult predictionResult = Prediction.Position.PredictLinearMissile(target, spells[SpellSlot.Q].Range, 90, 250, 2000, 0, Player.Instance.ServerPosition);
                        if (predictionResult.CollisionObjects.Count(x => x.IsValidTarget(Smite.Range) && x.IsMinion) ==
                            1 && CastSmite(predictionResult.CollisionObjects.FirstOrDefault()))
                        {
                            spells[SpellSlot.Q].Cast(target);
                            Console.WriteLine(predictionResult.HitChance);
                        }
                    }
                }
            }

            HandleW(true);

            if (comboMenu["combousee"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.E].IsReady())
            {
                AIHeroClient target = TargetSelector.GetTarget(spells[SpellSlot.E].Range, DamageType.Magical);

                if (target != null)
                    spells[SpellSlot.E].Cast();
            }
        }

        private static void Harass()
        {
            if (harassMenu["harassuseq"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.Q].IsReady())
            {
                AIHeroClient target = TargetSelector.GetTarget(spells[SpellSlot.Q].Range, DamageType.Magical);

                if (target != null && !Player.Instance.IsInAutoAttackRange(target))
                    spells[SpellSlot.Q].Cast(target);
            }

            HandleW();

            if (harassMenu["harassusee"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.E].IsReady())
            {
                AIHeroClient target = TargetSelector.GetTarget(spells[SpellSlot.E].Range, DamageType.Magical);

                if (target != null)
                    spells[SpellSlot.E].Cast();
            }
        }

        private static void LaneClear()
        {
            if (farmMenu["laneclearuseq"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.Q].IsReady())
            {
                Obj_AI_Base minion = EntityManager.GetLaneMinions(EntityManager.UnitTeam.Enemy, Player.Instance.ServerPosition.To2D(), spells[SpellSlot.Q].Range, false).OrderByDescending(x => x.MaxHealth).FirstOrDefault();

                if (minion != null && !Player.Instance.IsInAutoAttackRange(minion))
                    spells[SpellSlot.Q].Cast(minion);
            }

            HandleW();

            if (farmMenu["laneclearusee"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.E].IsReady())
            {
                int count = EntityManager.GetLaneMinions(EntityManager.UnitTeam.Enemy, Player.Instance.ServerPosition.To2D(), spells[SpellSlot.E].Range, false).Count;

                if (count >= 3)
                    spells[SpellSlot.E].Cast();
            }
        }

        private static void JungleClear()
        {
            if (farmMenu["jungleclearuseq"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.Q].IsReady())
            {
                Obj_AI_Base jungleMob = EntityManager.GetJungleMonsters(Player.Instance.ServerPosition.To2D(), spells[SpellSlot.Q].Range, false).OrderByDescending(x => x.MaxHealth).FirstOrDefault();


                if (jungleMob != null && !Player.Instance.IsInAutoAttackRange(jungleMob))
                    spells[SpellSlot.Q].Cast(jungleMob);
            }

            HandleW();

            if (farmMenu["jungleclearusee"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.E].IsReady())
            {
                int count = EntityManager.GetJungleMonsters(Player.Instance.ServerPosition.To2D(), spells[SpellSlot.E].Range, false).Count;

                if (count >= 1)
                    spells[SpellSlot.E].Cast();
            }
        }

        private static void AutoR()
        {
            if (comboMenu["combouser"].Cast<CheckBox>().CurrentValue && Player.Instance.CountEnemiesInRange(spells[SpellSlot.R].Range) >= comboMenu["comboautor"].Cast<Slider>().CurrentValue)
            {
                spells[SpellSlot.R].Cast();
            }
        }

        private static void HandleW(bool combomode = false)
        {
            if (autowMenu["autousew"].Cast<CheckBox>().CurrentValue && spells[SpellSlot.W].IsReady() && Player.Instance.ManaPercent >= autowMenu["autowmana"].Cast<Slider>().CurrentValue)
            {
                int minions = EntityManager.GetLaneMinions(EntityManager.UnitTeam.Enemy, Player.Instance.ServerPosition.To2D(), spells[SpellSlot.W].Range).Count;
                int jungleMobs = EntityManager.GetJungleMonsters(Player.Instance.ServerPosition.To2D(), spells[SpellSlot.W].Range).Count;

                if (combomode)
                {
                    AIHeroClient target = TargetSelector.GetTarget(spells[SpellSlot.W].Range, DamageType.Magical);

                    if (target != null && spells[SpellSlot.W].Handle.ToggleState == 1)
                        spells[SpellSlot.W].Cast();
                    else if (target == null && spells[SpellSlot.W].Handle.ToggleState == 2)
                        spells[SpellSlot.W].Cast();
                }
                else
                {
                    if ((minions >= 3 || jungleMobs >= 1) && spells[SpellSlot.W].Handle.ToggleState == 1)
                        spells[SpellSlot.W].Cast();
                    else if (minions < 3 && jungleMobs < 1 && spells[SpellSlot.W].Handle.ToggleState == 2)
                        spells[SpellSlot.W].Cast();
                }
            }
        }

        private static bool CastSmite(Obj_AI_Base target)
        {
            return Smite.IsReady() && (target.Health < Player.Instance.GetSummonerSpellDamage(target, DamageLibrary.SummonerSpells.Smite)) && Player.Instance.Spellbook.CastSpell(Smite.Slot, target);
        }
    }
}