using System;
using System.Collections.Generic;
using System.Linq;
using AutoHook.Classes;
using AutoHook.Configurations;
using AutoHook.Enums;
using AutoHook.Resources.Localization;
using AutoHook.Ui;
using AutoHook.Utils;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Utility;
using ImGuiNET;

namespace AutoHook;

public class PresetCreator
{
    private string namepreset = "";
    private Fish? selectedTargetFish;
    private bool includeTimers;
    private bool includeIntPrep;
    private PresetConfig newPreset = new("");

    private void DrawHeader()
    {
        ImGui.PushTextWrapPos();
        ImGui.TextColored(ImGuiColors.DalamudYellow,
            " !!! Experimental Feature !!! \nThis is not optimized at the moment and its just a starting point\nJoin the discord and leave a suggestion on how to improve");
        ImGui.PopTextWrapPos();

        DrawUtil.TextV("Selected the target fish");
        DrawUtil.DrawComboSelector(
            GameRes.ImportedFishes.Where(f => !f.IsSpearFish).ToList(),
            (Fish item) => item.Name,
            selectedTargetFish?.Name ?? UIStrings.None,
            (Fish item) => this.selectedTargetFish = item);

        DrawUtil.TextV("Preset Name: ");
        ImGui.SetNextItemWidth(220 * ImGuiHelpers.GlobalScale);
        if (ImGui.InputTextWithHint("###input", "Preset name", ref namepreset, 64, ImGuiInputTextFlags.AutoSelectAll))
        {
            
        }
    }

    public void PresetGenerator()
    {
        try
        {
            DrawHeader();

            if (selectedTargetFish == null)
                return;

            List<Fish>? moochList = [];
            List<(Fish, int)>? prepList = [];

            DrawUtil.SpacingSeparator();

            DrawUtil.TextV($"Initial Bait: {MultiString.GetItemName(selectedTargetFish.InitialBait)}");

            if (selectedTargetFish?.Mooches.Count > 0)
            {
                moochList = selectedTargetFish.Mooches
                    .Select(mooch => GameRes.ImportedFishes.FirstOrDefault(f => f.ItemId == mooch)).OfType<Fish>()
                    .ToList();

                DrawUtil.TextV(
                    $"Mooch order: {string.Join(" > ", moochList.Select(fish => $"{fish.Name} {GetBiteType(fish.BiteType)}"))}");
            }

            DrawUtil.Checkbox("Include fish hooking timers", ref includeTimers,
                "The values are based on the info available on TeamCraft and are not 100% accurate");

            if (selectedTargetFish?.Predators.Count > 0)
            {
                DrawUtil.Checkbox("Include intuition preparation in the same preset > READ", ref includeIntPrep,
                    "Even more experimental, works well with 1 fish requirement but 2 or more idk about that (will be improved)");

                if (includeIntPrep)
                {
                    foreach (var predator in selectedTargetFish.Predators)
                    {
                        var fish = GameRes.ImportedFishes.FirstOrDefault(f => f.ItemId == predator.itemId);

                        if (fish != null) prepList.Add((fish, predator.qtd));
                    }

                    DrawUtil.TextV($"Intuition Prep:\n{string.Join("\n", prepList.Select(fish
                        => $"{fish.Item2}x {fish.Item1.Name} {GetBiteType(fish.Item1.BiteType)} ({MultiString.GetItemName(fish.Item1.InitialBait)})"))}");
                }
            }

            /*if (includeTimers)
            {
                //DrawFishTimers(moochList, prepList);
            }*/

            if (ImGui.Button("Generate Preset and Close"))
            {
                newPreset = new PresetConfig("");

                if (selectedTargetFish == null)
                    return;

                var isInt = prepList?.Count > 0;

                if (namepreset == string.Empty)
                {
                    namepreset = $"Auto - {selectedTargetFish.Name} {DateTime.Now}";
                }

                newPreset.RenamePreset(namepreset);
                SetupBaitAndMooch(selectedTargetFish.InitialBait, selectedTargetFish, moochList, isInt);
                if (isInt)
                    SetupIntPrep(prepList!);

                newPreset.AddFishConfig(new FishConfig(selectedTargetFish.ItemId) { StopAfterCaught = true });

                newPreset.AutoCastsCfg.EnableAll = true;
                newPreset.AutoCastsCfg.CastLine.Enabled = true;
                newPreset.AutoCastsCfg.CastCordial.Enabled = true;
                newPreset.AutoCastsCfg.CastPatience.Enabled = true;
                newPreset.AutoCastsCfg.CastMakeShiftBait.Enabled = true;
                newPreset.AutoCastsCfg.CastLine.Enabled = true;

                Service.Configuration.HookPresets.CustomPresets.Add(newPreset);
                Service.Configuration.HookPresets.SelectedPreset = newPreset;

                namepreset = "";
                selectedTargetFish = null;
                Service.Save();
                TabCustomPresets.OpenPresetGen = false;
            }
        }
        catch (Exception e)
        {
            Service.PrintDebug(e.Message);
        }
    }
/*
    private double placeHolder;
    private double placeHolderMax;

    private void DrawFishTimers(List<Fish> moochList, List<(Fish, int)> prepList)
    {
        ImGui.Indent();
        if (selectedTargetFish != null)
        {
            var timer = GameRes.BiteTimers.FirstOrDefault(b => b.itemId == selectedTargetFish.ItemId) ?? new BiteTimers();
            placeHolder = timer!.min;
            placeHolderMax = timer.max;
            DrawUtil.TextV($"{selectedTargetFish.Name}");
            ImGui.SameLine();
            ImGui.SetNextItemWidth(40 * ImGuiHelpers.GlobalScale);
            if (ImGui.InputDouble("###min", ref placeHolder, 0, 0, @"%.1f%"))
            { }
            ImGui.SameLine();
            ImGui.Button(UIStrings.Min);
            ImGui.SameLine();
            ImGui.Text(" - ");
            ImGui.SameLine();
            ImGui.SetNextItemWidth(40 * ImGuiHelpers.GlobalScale);
            if (ImGui.InputDouble("###max", ref placeHolderMax, 0, 0, @"%.1f%"))
            { }
            ImGui.SameLine();
            ImGui.Button(UIStrings.Max);
        }

        ImGui.Unindent();
    }*/

    private void SetupIntPrep(List<(Fish, int)> intuiPrep)
    {
        foreach (var fishPrep in intuiPrep)
        {
            var fish = fishPrep.Item1;
            Service.PrintDebug($"Theres Prep: {fish.Name}, bait: {fish.InitialBait}, {GetBiteType(fish.BiteType)}");
            var mooches = fish.Mooches
                .Select(mooch => GameRes.ImportedFishes.FirstOrDefault(f => f.ItemId == mooch)).OfType<Fish>()
                .ToList();

            SetupBaitAndMooch(fish.InitialBait, fish, mooches);
        }
    }

    private void SetupBaitAndMooch(int bait, Fish fishTarget, List<Fish>? moochList, bool isIntuition = false)
    {
        var initialBaitCfg = newPreset.ListOfBaits.FirstOrDefault(f => f.BaitFish.Id == bait);

        if (initialBaitCfg == null)
        {
            initialBaitCfg = new HookConfig(bait);
            initialBaitCfg.ResetAllHooksets();
        }

        if (isIntuition)
            initialBaitCfg.IntuitionHook.UseCustomStatusHook = true;

        // if theres no mooch, set the bait to hook the Tug from the target fish
        if (moochList == null || moochList.Count == 0)
        {
            initialBaitCfg.SetBiteAndHookType(fishTarget!.BiteType,fishTarget!.HookType, isIntuition);

            if (includeTimers)
            {
                var timer = GameRes.BiteTimers.FirstOrDefault(b => b.itemId == fishTarget.ItemId) ?? new BiteTimers();
                initialBaitCfg.SetHooksetTimer(fishTarget.BiteType, timer.q1, timer.q3, isIntuition);
            }

            newPreset.ReplaceBaitConfig(initialBaitCfg);
            return;
        }

        // the list is going backwards to make it easier
        moochList.Reverse();

        foreach (var mooch in moochList)
        {
            // check if the mooch is already included in the list
            var newMooch = newPreset.ListOfMooch.FirstOrDefault(f => f.BaitFish.Id == mooch.ItemId);

            if (newMooch == null)
            {
                newMooch = new HookConfig(mooch.ItemId);
                newMooch.ResetAllHooksets();
            }

            if (isIntuition)
                newMooch.IntuitionHook.UseCustomStatusHook = true;

            // Add the fish to the Fish Caught tab and enable Auto Mooch I/II
            var fishConfig = new FishConfig(mooch.ItemId);
            fishConfig.Mooch.Enabled = true;
            fishConfig.Mooch.Mooch2.Enabled = true;
            newPreset.AddFishConfig(fishConfig);

            Fish nextFish;

            // target fish < last mooch < other mooches < first mooch < bait
            // in other words, the bait needs to know the BiteType of the first mooch and the last mooch needs to know the bite of the target fish
            // The list is reversed so we can setup more easily
            if (mooch == moochList.First())
                nextFish = fishTarget;
            else if (mooch == moochList.Last())
                nextFish = moochList[^2];
            else
                nextFish = moochList[moochList.IndexOf(mooch) - 1];

            // only hook the next fish BiteType
            // REMEMBER YOU FUCK, THE NEXT FISH IS THE PREVIOUS ONE IN THE LIST
            newMooch.SetBiteAndHookType(nextFish.BiteType, nextFish.HookType, isIntuition);

            if (includeTimers)
            {
                var timer = GameRes.BiteTimers.FirstOrDefault(b => b.itemId == nextFish.ItemId) ?? new BiteTimers();
                newMooch.SetHooksetTimer(nextFish.BiteType, timer.q1, timer.q3, isIntuition);
            }

            newPreset.ReplaceMoochConfig(newMooch);

            // the last fish in the list is the first one being hooked
            if (mooch == moochList.Last())
            {
                // that means we need to set up the bait to the this fish bite.
                initialBaitCfg.SetBiteAndHookType(mooch.BiteType, mooch.HookType, isIntuition);
                if (includeTimers)
                {
                    var timer = GameRes.BiteTimers.FirstOrDefault(b => b.itemId == mooch.ItemId) ?? new BiteTimers();
                    initialBaitCfg.SetHooksetTimer(mooch.BiteType, timer.q1, timer.q3, isIntuition);
                }

                newPreset.ReplaceBaitConfig(initialBaitCfg);
            }
        }
    }

    private static string GetBiteType(BiteType bite)
        => bite switch
        {
            BiteType.Weak => "(!)",
            BiteType.Strong => "(!!)",
            BiteType.Legendary => "(!!!)",
            _ => "idk what happened this shouldnt be here im dont care anymore im going to explode",
        };
}