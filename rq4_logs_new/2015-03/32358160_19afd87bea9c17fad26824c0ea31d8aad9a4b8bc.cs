using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LeagueSharp.Common;
using LeagueSharp;
using Color = System.Drawing.Color;


namespace DJNunu
{
    class Program
    {
        // Fields and Functions
        private static Orbwalking.Orbwalker Orbwalker;
        private static Menu Menu;
        private static Spell Q, W, E, R;
        private static SpellSlot Ignite;
        private static Obj_AI_Hero Player;
        private static float IgniteDamage(Obj_AI_Base target)
        {
            if (Ignite == SpellSlot.Unknown || Player.Spellbook.CanUseSpell(Ignite) != SpellState.Ready)
            {
                return 0f;
            }
            return (float)Player.GetSummonerSpellDamage(target, Damage.SummonerSpell.Ignite);
        }
        static void Main(string[] args)
        {
            CustomEvents.Game.OnGameLoad += Game_OnGameLoad;
        }
        static void Game_OnGameLoad(EventArgs args)
        {
            if (ObjectManager.Player.ChampionName != "Nunu")
            {
                return;
            }
            Player = ObjectManager.Player;
            Notifications.AddNotification("DJ Nunu by xMusic", 10000);
            Q = new Spell(SpellSlot.Q, 125);
            W = new Spell(SpellSlot.W, 700);
            E = new Spell(SpellSlot.E, 550);
            R = new Spell(SpellSlot.R, 650);
            Ignite = Player.GetSpellSlot("summonerdot");

            // Menu
            Menu = new Menu("DJ Nunu", "Nunu", true);
            var tsMenu = new Menu("Target Selector", "Target Selector");
            TargetSelector.AddToMenu(tsMenu);
            Menu.AddSubMenu(tsMenu);
            Menu.AddSubMenu(new Menu("Orbwalker", "Orbwalker"));
            Orbwalker = new Orbwalking.Orbwalker(Menu.SubMenu("Orbwalking"));

            Menu.AddSubMenu(new Menu("Combo", "Combo"));
            Menu.SubMenu("Combo").AddItem(new MenuItem("Use W", "wc").SetValue(true));
            Menu.SubMenu("Combo").AddItem(new MenuItem("Use E", "ec").SetValue(true));
            Menu.SubMenu("Combo").AddItem(new MenuItem("Use R", "rc").SetValue(true));
            Menu.SubMenu("Combo").AddItem(new MenuItem("Enemies for R?", "enemiesr").SetValue(new Slider(3, 1, 5)));
            Menu.SubMenu("Combo").AddItem(new MenuItem("Use Ignite", "ignitec").SetValue(true));

            Menu.AddSubMenu(new Menu("Harass", "Harass"));
            Menu.SubMenu("Harass").AddItem(new MenuItem("Use W", "wh").SetValue(true));
            Menu.SubMenu("Harass").AddItem(new MenuItem("Use E", "eh").SetValue(true));

            Menu.AddSubMenu(new Menu("Lane Clear", "LaneClear"));
            Menu.SubMenu("Lane Clear").AddItem(new MenuItem("Use Q", "qlc").SetValue(true));
            Menu.SubMenu("Lane Clear").AddItem(new MenuItem("Health Manager", "lanehm").SetValue(new Slider(50, 1, 100)));
            Menu.SubMenu("Lane Clear").AddItem(new MenuItem("Use W", "wlc").SetValue(true));
            Menu.SubMenu("Lane Clear").AddItem(new MenuItem("Use E", "elc").SetValue(true));

            Menu.AddSubMenu(new Menu("Jungle Clear", "JungleClear"));
            Menu.SubMenu("Jungle Clear").AddItem(new MenuItem("Use Q", "qjc").SetValue(true));
            Menu.SubMenu("Jungle Clear").AddItem(new MenuItem("Health Manager", "junglehm").SetValue(new Slider(50, 1, 100)));
            Menu.SubMenu("Jungle Clear").AddItem(new MenuItem("Use W", "wjc").SetValue(true));
            Menu.SubMenu("Jungle Clear").AddItem(new MenuItem("Use E", "ejc").SetValue(true));

            Menu.AddSubMenu(new Menu("Last Hit", "LastHit"));
            Menu.SubMenu("Last Hit").AddItem(new MenuItem("Use Q", "qlh").SetValue(true));
            Menu.SubMenu("Last Hit").AddItem(new MenuItem("Use E", "elh").SetValue(true));
            Menu.SubMenu("Last Hit").AddItem(new MenuItem("Last Hit Key", "lasthitkeybinding").SetValue(new KeyBind("T".ToCharArray()[0], KeyBindType.Press)));

            Menu.AddSubMenu(new Menu("Kill Steal", "KillSteal"));
            Menu.SubMenu("Kill Steal").AddItem(new MenuItem("Use E", "eks").SetValue(true));
            Menu.SubMenu("Kill Steal").AddItem(new MenuItem("Use R", "rks").SetValue(true));
            Menu.SubMenu("Kill Steal").AddItem(new MenuItem("Use Ignite", "igniteks").SetValue(true));

            Menu.AddSubMenu(new Menu("Misc", "Misc"));
            Menu.SubMenu("Misc").AddItem(new MenuItem("Auto Q", "autoq").SetValue(true));
            Menu.SubMenu("Misc").AddItem(new MenuItem("Health Manager", "mischm").SetValue(new Slider(50, 1, 100)));
            Menu.SubMenu("Misc").AddItem(new MenuItem("Use W on Allies", "wallies").SetValue(true));
            Menu.SubMenu("Misc").AddItem(new MenuItem("Use Igntie", "miscignite").SetValue(new StringList(new[] { "Combo", "Kill Steal" })));

            Menu.AddSubMenu(new Menu("Drawings", "Drawings"));
            Menu.SubMenu("Drawings").AddItem(new MenuItem("Draw Q", "dq").SetValue(true));
            Menu.SubMenu("Drawings").AddItem(new MenuItem("Draw W", "dw").SetValue(true));
            Menu.SubMenu("Drawings").AddItem(new MenuItem("Draw E", "de").SetValue(true));
            Menu.SubMenu("Drawings").AddItem(new MenuItem("Draw R", "dr").SetValue(true));

            Menu.AddToMainMenu();
            Game.OnGameUpdate += Game_OnGameUpdate;
            Drawing.OnDraw += Drawing_OnDraw;
        }

        // Methods
        static void Drawing_OnDraw(EventArgs args)
        {
            if (Menu.Item("dq").GetValue<bool>())
            {
                Render.Circle.DrawCircle(ObjectManager.Player.Position, Q.Range, System.Drawing.Color.BlueViolet);
            }
            if (Menu.Item("dw").GetValue<bool>())
            {
                Render.Circle.DrawCircle(ObjectManager.Player.Position, W.Range, System.Drawing.Color.DeepSkyBlue);
            }
            if (Menu.Item("de").GetValue<bool>())
            {
                Render.Circle.DrawCircle(ObjectManager.Player.Position, E.Range, System.Drawing.Color.RoyalBlue);
            }
            if (Menu.Item("dr").GetValue<bool>())
            {
                Render.Circle.DrawCircle(ObjectManager.Player.Position, R.Range, System.Drawing.Color.AliceBlue);
            }
        }
        static void Game_OnGameUpdate(EventArgs args)
        {
            switch (Orbwalker.ActiveMode)
            {
                case Orbwalking.OrbwalkingMode.Combo:
                    Combo(TargetSelector.GetTarget(1200f, TargetSelector.DamageType.Magical));
                    break;
                case Orbwalking.OrbwalkingMode.Mixed:
                    Harass(TargetSelector.GetTarget(1200f, TargetSelector.DamageType.Magical));
                    break;
                case Orbwalking.OrbwalkingMode.LaneClear:
                    LaneClear();
                    JungleClear();
                    break;
            }
            LastHit();
            var ksTarget = ObjectManager.Get<Obj_AI_Hero>().Where(t => t.IsValidTarget()).OrderBy(t => t.Health).FirstOrDefault();
            if (ksTarget != null)
                KillSteal(ksTarget);
        }
        static void Combo(Obj_AI_Hero target)
        {
            if (target == null)
            {
                return;
            }
            if (Menu.Item("wc").GetValue<bool>() && Menu.Item("wallies").GetValue<bool>() && W.IsReady())
            {
                foreach (var allies in ObjectManager.Get<Obj_AI_Hero>().Where(a => a.IsAlly && a.IsValid))
                {
                    W.CastOnBestTarget();
                    break;
                }
            }
            if (Menu.Item("ec").GetValue<bool>() && E.IsReady())
            {
                E.CastOnUnit(target);
            }
            var enemyCount = ObjectManager.Get<Obj_AI_Hero>().Count(e => e.IsValidTarget(R.Range));
            if (Menu.Item("rc").GetValue<bool>() && enemyCount >= Menu.Item("enemiesr").GetValue<Slider>().Value && R.IsReady() && R.IsReady())
            {
                R.Cast();
            }
        }
        static void Harass(Obj_AI_Hero target)
        {
            if (target == null)
            {
                return;
            }
            if (Menu.Item("wh").GetValue<bool>() && Menu.Item("wallies").GetValue<bool>() && W.IsReady())
            {
                foreach (var allies in ObjectManager.Get<Obj_AI_Hero>().Where(a => a.IsAlly && a.IsValid))
                {
                    W.CastOnBestTarget();
                    break;
                }
            }
            if (Menu.Item("eh").GetValue<bool>() && E.IsReady())
            {
                E.CastOnUnit(target);
            }
        }
        static void LaneClear()
        {
            var allMinions = MinionManager.GetMinions(ObjectManager.Player.ServerPosition, Q.Range);
            if (Player.HealthPercentage() < Menu.Item("lanehm").GetValue<Slider>().Value)
            {
                if (Menu.Item("qlc").GetValue<bool>() && Q.IsReady())
                {
                    foreach (var minion in allMinions)
                    {
                        if (minion.IsValidTarget())
                        {
                            Q.CastOnUnit(minion);
                        }
                    }
                }
            }
            if (Menu.Item("wlc").GetValue<bool>() && Menu.Item("wallies").GetValue<bool>() && W.IsReady())
            {
                foreach (var allies in ObjectManager.Get<Obj_AI_Hero>().Where(a => a.IsAlly && a.IsValid))
                {
                    W.CastOnBestTarget();
                    break;
                }
            }
            if (Menu.Item("elc").GetValue<bool>() && E.IsReady())
            {
                foreach (var minion in allMinions)
                {
                    if (minion.IsValidTarget())
                    {
                        E.CastOnUnit(minion);
                    }
                }
            }
        }
        static void JungleClear()
        {
            var allMinions = MinionManager.GetMinions(ObjectManager.Player.ServerPosition, Q.Range);
            if (Player.HealthPercentage() < Menu.Item("junglehm").GetValue<Slider>().Value)
            {
                if (Menu.Item("qjc").GetValue<bool>() && Q.IsReady())
                {
                    foreach (var minion in allMinions)
                    {
                        if (minion.IsValidTarget())
                        {
                            Q.CastOnUnit(minion);
                        }
                    }
                }
            }
            if (Menu.Item("wjc").GetValue<bool>() && Menu.Item("wallies").GetValue<bool>() && W.IsReady())
            {
                foreach (var allies in ObjectManager.Get<Obj_AI_Hero>().Where(a => a.IsAlly && a.IsValid))
                {
                    W.CastOnBestTarget();
                    break;
                }
            }
            if (Menu.Item("ejc").GetValue<bool>() && E.IsReady())
            {
                foreach (var minion in allMinions)
                {
                    if (minion.IsValidTarget())
                    {
                        E.CastOnUnit(minion);
                    }
                }
            }
        }
        static void LastHit()
        {
            var keyActive = Menu.Item("lasthitkeybinding").GetValue<KeyBind>().Active;
            if (!keyActive)
                return;
            var allMinions = MinionManager.GetMinions(ObjectManager.Player.ServerPosition, Q.Range);
            if (Menu.Item("qlh").GetValue<bool>() && Q.IsReady())
            {
                foreach (var minion in allMinions.Where(minion => minion.Health <= ObjectManager.Player.GetSpellDamage(minion, SpellSlot.Q)))
                {
                    if (minion.IsValidTarget())
                    {
                        Q.CastOnUnit(minion);
                    }
                }
            }
            if (Menu.Item("elh").GetValue<bool>() && E.IsReady())
            {
                foreach (var minion in allMinions.Where(minion => minion.Health <= ObjectManager.Player.GetSpellDamage(minion, SpellSlot.E)))
                {
                    if (minion.IsValidTarget())
                    {
                        E.CastOnUnit(minion);
                    }
                }
            }
        }
        static void KillSteal(Obj_AI_Hero Target)
        {
            var Champions = ObjectManager.Get<Obj_AI_Hero>();
            if (Menu.Item("eks").GetValue<bool>() && E.IsReady())
            {
                foreach (var champ in Champions.Where(champ => champ.Health <= ObjectManager.Player.GetSpellDamage(champ, SpellSlot.E)))
                {
                    if (champ.IsValidTarget())
                    {
                        E.CastOnUnit(champ);
                    }
                }
            }
            if (Menu.Item("rks").GetValue<bool>() && E.IsReady())
            {
                foreach (var champ in Champions.Where(champ => champ.Health <= ObjectManager.Player.GetSpellDamage(champ, SpellSlot.E)))
                {
                    if (champ.IsValidTarget())
                    {
                        E.CastOnUnit(champ);
                    }
                }
            }
            if (Player.Distance(Target.Position) <= 600 && IgniteDamage(Target) >= Target.Health && Menu.Item("ignitec").GetValue<bool>() && Menu.Item("igniteks").GetValue<bool>() && Menu.Item("miscignite").GetValue<StringList>().SelectedIndex == 1)
            {
                Player.Spellbook.CastSpell(Ignite, Target);
            }
        }
        static void Misc()
        {
            var allMinions = MinionManager.GetMinions(ObjectManager.Player.ServerPosition, Q.Range);
            if (Player.HealthPercentage() < Menu.Item("mischm").GetValue<Slider>().Value)
            {
                Q.Cast(allMinions.Where(m => m.IsValidTarget()).OrderBy(m => m.Health).FirstOrDefault());
            }
        }
    }
}