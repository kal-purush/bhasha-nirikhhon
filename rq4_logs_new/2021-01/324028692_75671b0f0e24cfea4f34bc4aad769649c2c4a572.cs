using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using RandomizerLib.MultiWorld;

namespace RandomizerLib.Logging
{
    public class SpoilerLogger
    {
        private string path;
       
        public SpoilerLogger(string path)
        {
            this.path = path;
        }

        public void LogSpoiler(string message)
        {
            File.AppendAllText(Path.Combine(path, "RandomizerSpoilerLog.txt"), message + Environment.NewLine);
        }

        public void InitializeSpoiler(RandoResult result)
        {
            File.Create(Path.Combine(path, "RandomizerSpoilerLog.txt")).Dispose();
            LogSpoiler("Randomization completed with seed: " + result.settings.Seed);

            if (result.players > 1)
            {
                LogSpoiler("MW Players:");
                for (int i = 0; i < result.nicknames.Count; i++)
                {
                    LogSpoiler($"{i + 1}: {result.nicknames[i]}");
                }
            }
        }

        public void LogAllToSpoiler(RandoResult result)
        {
            new Thread(() =>
            {
                Stopwatch spoilerWatch = new Stopwatch();
                spoilerWatch.Start();
                string log = SpoilerLogger.generateSpoilerLog(result);

                spoilerWatch.Stop();
                LogSpoiler(log);
                LogSpoiler("Generated spoiler log in " + spoilerWatch.Elapsed.TotalSeconds + " seconds.");
            }).Start();
        }

        public static string generateSpoilerLog(RandoResult result)
        {
            string log = string.Empty;
            void AddToLog(string message) => log += message + Environment.NewLine;

            AddToLog(GetItemSpoiler(result));
            if (result.settings.RandomizeTransitions) AddToLog(GetTransitionSpoiler(result.settings, result.transitionPlacements.Select(kvp => (kvp.Key, kvp.Value)).ToArray()));

            try
            {
                AddToLog(Environment.NewLine + "SETTINGS");
                AddToLog($"Seed: {result.settings.Seed}");
                AddToLog($"Mode: " + // :)
                    $"{(result.settings.RandomizeRooms ? (result.settings.ConnectAreas ? "Connected-Area Room Randomizer" : "Room Randomizer") : (result.settings.RandomizeAreas ? "Area Randomizer" : "Item Randomizer"))}");
                if (result.players > 1)
                {
                    AddToLog($"Multiworld players: {result.players}");
                    AddToLog($"Multiworld player ID: {result.playerId + 1}");
                }
                AddToLog($"Cursed: {result.settings.Cursed}");
                AddToLog($"Start location: {result.settings.StartName}");
                AddToLog($"Random start items: {result.settings.RandomizeStartItems}");
                AddToLog("REQUIRED SKIPS");
                AddToLog($"Mild skips: {result.settings.MildSkips}");
                AddToLog($"Shade skips: {result.settings.ShadeSkips}");
                AddToLog($"Fireball skips: {result.settings.FireballSkips}");
                AddToLog($"Acid skips: {result.settings.AcidSkips}");
                AddToLog($"Spike tunnels: {result.settings.SpikeTunnels}");
                AddToLog($"Dark Rooms: {result.settings.DarkRooms}");
                AddToLog($"Spicy skips: {result.settings.SpicySkips}");
                AddToLog("RANDOMIZED LOCATIONS");
                AddToLog($"Dreamers: {result.settings.RandomizeDreamers}");
                AddToLog($"Skills: {result.settings.RandomizeSkills}");
                AddToLog($"Charms: {result.settings.RandomizeCharms}");
                AddToLog($"Keys: {result.settings.RandomizeKeys}");
                AddToLog($"Geo chests: {result.settings.RandomizeGeoChests}");
                AddToLog($"Mask shards: {result.settings.RandomizeMaskShards}");
                AddToLog($"Vessel fragments: {result.settings.RandomizeVesselFragments}");
                AddToLog($"Pale ore: {result.settings.RandomizePaleOre}");
                AddToLog($"Charm notches: {result.settings.RandomizeCharmNotches}");
                AddToLog($"Rancid eggs: {result.settings.RandomizeRancidEggs}");
                AddToLog($"Relics: {result.settings.RandomizeRelics}");
                AddToLog($"Stags: {result.settings.RandomizeStags}");
                AddToLog($"Maps: {result.settings.RandomizeMaps}");
                AddToLog($"Grubs: {result.settings.RandomizeGrubs}");
                AddToLog($"Whispering roots: {result.settings.RandomizeWhisperingRoots}");
                AddToLog($"Lifeblood cocoons: {result.settings.RandomizeLifebloodCocoons}");
                AddToLog($"Duplicate major items: {result.settings.DuplicateMajorItems}");
                AddToLog("QUALITY OF LIFE");
                AddToLog($"Grubfather: {result.settings.Grubfather}");
                AddToLog($"Salubra: {result.settings.CharmNotch}");
                AddToLog($"Early geo: {result.settings.EarlyGeo}");
                AddToLog($"Extra platforms: {result.settings.ExtraPlatforms}");
                AddToLog($"Levers: {result.settings.LeverSkips}");
                AddToLog($"Jiji: {result.settings.Jiji}");
            }
            catch
            {
                AddToLog("Error logging randomizer settings!?!?");
            }

            return log;
        }

        private static string GetItemSpoiler(RandoResult result)
        {
            string log = string.Empty;
            void AddToLog(string message) => log += message + Environment.NewLine;

            // If this is a single player world, leave out names
            string MWItemToString(MWItem item) => result.players > 1 ? $"{result.nicknames[item.PlayerId]}-{item.Item}" : item.Item;

            (int, MWItem, MWItem)[] orderedILPairs = new (int, MWItem, MWItem)[result.itemPlacements.Count];

            int i = 0;
            foreach (var kvp in result.itemPlacements)
            {
                orderedILPairs[i++] = (result.locationOrder[kvp.Value], kvp.Key, kvp.Value);
            }

            try
            {
                orderedILPairs = orderedILPairs.OrderBy(triplet => triplet.Item1).ToArray();

                Dictionary<MWItem, List<string>> areaItemLocations = new Dictionary<MWItem, List<string>>();
                foreach (var triplet in orderedILPairs)
                {
                    MWItem location = triplet.Item3;
                    if (LogicManager.TryGetItemDef(location, out ReqDef locationDef))
                    {
                        MWItem area = new MWItem(location.PlayerId, locationDef.areaName);
                        if (!areaItemLocations.ContainsKey(area))
                        {
                            areaItemLocations[area] = new List<string>();
                        }
                    }
                    else if (!areaItemLocations.ContainsKey(location))
                    {
                        areaItemLocations[location] = new List<string>();
                    }
                }

                List<string> progression = new List<string>();
                foreach ((int, MWItem, MWItem) pair in orderedILPairs)
                {
                    string cost = "";
                    if (LogicManager.TryGetItemDef(pair.Item3, out ReqDef itemDef)) {
                        if (itemDef.cost != 0) cost = $" [{(result.variableCosts.ContainsKey(pair.Item3) ? result.variableCosts[pair.Item3] : itemDef.cost)} {itemDef.costType.ToString("g")}]";
                    }
                    else cost = $" [{result.shopCosts[pair.Item2]} Geo]";

                    if (LogicManager.GetItemDef(pair.Item2.Item).progression) progression.Add($"({pair.Item1}) {MWItemToString(pair.Item2)}<---at--->{MWItemToString(pair.Item3)}{cost}");
                    if (LogicManager.TryGetItemDef(pair.Item3, out ReqDef locationDef))
                    {
                        areaItemLocations[new MWItem(pair.Item3.PlayerId, locationDef.areaName)].Add($"({pair.Item1}) {MWItemToString(pair.Item2)}<---at--->{MWItemToString(pair.Item3)}{cost}");
                    }
                    else areaItemLocations[pair.Item3].Add($"{MWItemToString(pair.Item2)}{cost}");
                }

                AddToLog(Environment.NewLine + "PROGRESSION ITEMS");
                foreach (string item in progression) AddToLog(item.Replace('_', ' '));

                AddToLog(Environment.NewLine + "ALL ITEMS");
                foreach (MWItem key in areaItemLocations.Keys.OrderBy(mwItem => mwItem.PlayerId))
                {
                    List<string> val = areaItemLocations[key];
                    if (val.Count > 0)
                    {
                        string title = key.Item;
                        title = CleanAreaName(title);
                        if (result.players > 1) title = $"{result.nicknames[key.PlayerId]} {title}";
                        if (LogicManager.ShopNames.Contains(key.Item)) title = $"({orderedILPairs.First(triplet => triplet.Item3 == key).Item1}) {title}";
                        AddToLog(Environment.NewLine + title + ":");
                        foreach (string item in val) AddToLog(item.Replace('_', ' '));
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error while creating item spoiler log: " + e);
            }
            return log;
        }

        private static string GetTransitionSpoiler(RandoSettings settings, (string, string)[] transitionPlacements)
        {
            string log = string.Empty;
            void AddToLog(string message) => log += message + Environment.NewLine;

            try
            {
                if (settings.RandomizeAreas)
                {
                    Dictionary<string, List<string>> areaTransitions = new Dictionary<string, List<string>>();
                    foreach (string transition in LogicManager.TransitionNames(settings))
                    {
                        string area = LogicManager.GetTransitionDef(transition, settings).areaName;
                        if (!areaTransitions.ContainsKey(area))
                        {
                            areaTransitions[area] = new List<string>();
                        }
                    }

                    foreach ((string, string) pair in transitionPlacements)
                    {
                        string area = LogicManager.GetTransitionDef(pair.Item1, settings).areaName;
                        areaTransitions[area].Add(pair.Item1 + " --> " + pair.Item2);
                    }

                    AddToLog(Environment.NewLine + "TRANSITIONS");
                    foreach (KeyValuePair<string, List<string>> kvp in areaTransitions)
                    {
                        if (kvp.Value.Count > 0)
                        {
                            AddToLog(Environment.NewLine + kvp.Key.Replace('_', ' ') + ":");
                            foreach (string transition in kvp.Value) AddToLog(transition);
                        }
                    }
                }

                if (settings.RandomizeRooms)
                {
                    Dictionary<string, List<string>> roomTransitions = new Dictionary<string, List<string>>();
                    foreach (string transition in LogicManager.TransitionNames(settings))
                    {
                        string room = LogicManager.GetTransitionDef(transition, settings).sceneName;
                        if (!roomTransitions.ContainsKey(room))
                        {
                            roomTransitions[room] = new List<string>();
                        }
                    }

                    foreach ((string, string) pair in transitionPlacements)
                    {
                        string room = LogicManager.GetTransitionDef(pair.Item1, settings).sceneName;
                        roomTransitions[room].Add(pair.Item1 + " --> " + pair.Item2);
                    }

                    AddToLog(Environment.NewLine + "TRANSITIONS");
                    foreach (KeyValuePair<string, List<string>> kvp in roomTransitions)
                    {
                        if (kvp.Value.Count > 0)
                        {
                            AddToLog(Environment.NewLine + kvp.Key.Replace('_', ' ') + ":");
                            foreach (string transition in kvp.Value) AddToLog(transition);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Error while creating transition spoiler log: " + e);
            }
            return log;
        }

        public static string CleanAreaName(string name)
        {
            string newName = name.Replace('_', ' ');
            switch (newName)
            {
                case "Kings Pass":
                    newName = "King's Pass";
                    break;
                case "Queens Station":
                    newName = "Queen's Station";
                    break;
                case "Kings Station":
                    newName = "King's Station";
                    break;
                case "Queens Gardens":
                    newName = "Queen's Gardens";
                    break;
                case "Hallownests Crown":
                    newName = "Hallownest's Crown";
                    break;
                case "Kingdoms Edge":
                    newName = "Kingdom's Edge";
                    break;
                case "Weavers Den":
                    newName = "Weaver's Den";
                    break;
                case "Beasts Den":
                    newName = "Beast's Den";
                    break;
                case "Spirits Glade":
                    newName = "Spirit's Glade";
                    break;
                case "Ismas Grove":
                    newName = "Isma's Grove";
                    break;
                case "Teachers Archives":
                    newName = "Teacher's Archives";
                    break;
            }
            return newName;
        }
    }
}