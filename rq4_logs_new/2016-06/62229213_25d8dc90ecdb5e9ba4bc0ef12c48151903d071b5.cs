

namespace MultiProject
{
    using System.Diagnostics;
    using System.Reflection;
    using System.Collections.Generic;
    using Advanced_Combat_Tracker;
    using System;
    public static class ActHelper
    {
        private static object lockObject = new object();
        private static object ff14Plugin;
        private static object ff14PluginMemory;
        private static dynamic ff14PluginConfig;
        private static dynamic ff14PluginScancombat;

        public static bool Initialize()
        {

            lock (lockObject)
            {
                if (!ActGlobals.oFormActMain.Visible)
                {
                    return false;
                }

                ff14Plugin = ff14Plugin ?? FF14Plugin();
                if (ff14Plugin != null)
                {
                    ff14PluginMemory = ff14PluginMemory ?? FF14PluginMemory();

                    if (ff14PluginMemory != null && ff14PluginConfig == null)
                    {
                        FieldInfo fieldInfo;
                        fieldInfo = ff14PluginMemory.GetType().GetField("_config", BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.Instance);
                        ff14PluginConfig = fieldInfo.GetValue(ff14PluginMemory);
                    }
                    if (ff14PluginConfig !=null && ff14PluginScancombat == null)
                    {
                        FieldInfo fieldInfo;
                        fieldInfo = ff14PluginConfig.GetType().GetField("ScanCombatants", BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.Instance);
                        ff14PluginScancombat = fieldInfo.GetValue(ff14PluginConfig);
                    }

                    if(ff14PluginConfig!=null && ff14PluginScancombat != null)
                    {
                        return true;
                    }
                    return false;
                }
                else
                {
                    Trace.WriteLine("Error!, FFXIV_ACT_Plugin.dll not found.");
                    return false;
                }
            }
        }

        private static Object FF14Plugin()
        {
            foreach (var item in ActGlobals.oFormActMain.ActPlugins)
            {
                if (item.pluginFile.Name.ToUpper() == "FFXIV_ACT_Plugin.dll".ToUpper() &&
                    item.lblPluginStatus.Text.ToUpper() == "FFXIV Plugin Started.".ToUpper())
                {
                    return item.pluginObj;
                }
            }
            return null;
        }

        private static Object FF14PluginMemory()
        {
            FieldInfo fieldInfo;
            fieldInfo = ff14Plugin.GetType().GetField("_Memory", BindingFlags.GetField | BindingFlags.NonPublic | BindingFlags.Instance);
            return fieldInfo.GetValue(ff14Plugin);
        }


        public static Combatant GetCombatantPlayer()
        {
            var result = default(Combatant);

            if (Initialize())
            {
                /*
                #if DEBUG
                            result = new Combatant();
                            result.Job = 25;
                            result.CurrentMP = 1000;
                            result.MaxMP = 5400;
                            return result;
                #else
                */

                if (ff14PluginConfig.Process == null)
                {
                    return result;
                }

                object[] list = ff14PluginScancombat.GetCombatantList().ToArray();
                if (list.Length > 0)
                {
                    var item = (dynamic)list[0];
                    var combatant = new Combatant();

                    combatant.Name = (string)item.Name;
                    combatant.ID = (uint)item.ID;
                    combatant.Job = (int)item.Job;
                    combatant.CurrentMP = (int)item.CurrentMP;
                    combatant.MaxMP = (int)item.MaxMP;

                    result = combatant;
                }

                return result;
            }
            return result;
            /*
            #endif
            */
        }

        public static List<Combatant> GetCombatantList()
        {
            var result = new List<Combatant>();

            if (Initialize())
            {
                if (ff14PluginConfig.Process == null)
                {
                    return result;
                }
                dynamic list = ff14PluginScancombat.GetCombatantList();
                foreach (dynamic item in list.ToArray())
                {
                    if (item == null)
                    {
                        continue;
                    }

                    var combatant = new Combatant();

                    combatant.Name = (string)item.Name;
                    combatant.ID = (uint)item.ID;
                    combatant.Job = (int)item.Job;
                    combatant.CurrentMP = (int)item.CurrentMP;
                    combatant.MaxMP = (int)item.MaxMP;
                    combatant.CastTargetID = (int)item.CastTargetID;
                    combatant.IsCasting = (bool)item.IsCasting;
                    combatant.OwnerID = (uint)item.OwnerID;
                    combatant.Name = (string)item.Name;
                    combatant.type = (byte)item.type;
                    combatant.Level = (int)item.Level;
                    combatant.CurrentHP = (int)item.CurrentHP;
                    combatant.MaxHP = (int)item.MaxHP;
                    combatant.CurrentTP = (int)item.CurrentTP;
                    combatant.PosX = (float)item.PosX;
                    combatant.PosY = (float)item.PosY;

                    result.Add(combatant);
                }
            }
            return result;
        }

    }

    public class Combatant
    {
        public Combatant()
        {
            this.Name = string.Empty;
        }
        public uint ID;
        public uint OwnerID;
        public int Order;
        public byte type;
        public int Job;
        public int Level;
        public string Name;
        public int CurrentHP;
        public int MaxHP;
        public int CurrentMP;
        public int MaxMP;
        public int CurrentTP;
        public float PosX;
        public float PosY;
        public int CastTargetID;
        public bool IsCasting;

    }

    public class Player
    {
        public int JobID;
        public int Str;
        public int Dex;
        public int Vit;
        public int Intel;
        public int Mnd;
        public int Pie;
        public int Attack;
        public int Accuracy;
        public int Crit;
        public int AttackMagicPotency;
        public int HealMagicPotency;
        public int Det;
        public int SkillSpeed;
        public int SpellSpeed;
        public int WeaponDmg;
    }
}