using System;
using CrossUp.Commands;
using CrossUp.Features;
using CrossUp.Features.Layout;
using CrossUp.Game.Hooks;
using CrossUp.Utility;
using Dalamud.Interface;
using Dalamud.Interface.Colors;
using Dalamud.Interface.Components;
using FFXIVClientStructs.FFXIV.Common.Math;
using ImGuiNET;
using static CrossUp.CrossUp;
using static CrossUp.Utility.Service;

namespace CrossUp.UI.Tabs;

internal class SlotConfig
{
    public static void DrawTab()
    {
        if (!ImGui.BeginTabItem(Strings.SlotConfig.TabTitle)) return;

        ImGui.Spacing();

        if (ImGui.BeginTabBar("SlotConfigSubTabs"))
        {
            if (ImGui.BeginTabItem(Strings.SlotConfig.idle)) //if (ImGui.BeginTabItem(Strings.SlotConfig.Config))
            {

                ImGui.Spacing();
                ImGui.Indent(10);

                if (ImGui.BeginTable("Layout", 6, ImGuiTableFlags.SizingFixedFit | ImGuiTableFlags.ScrollX))
                {
                    ImGui.TableSetupColumn("labels", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("labels2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset2", ImGuiTableColumnFlags.WidthFixed);

                    ImGui.TableNextRow();
                    ImGui.TableNextColumn();
                    Rows.MainSettings();

                    ImGui.EndTable();
                }

                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem(Strings.SlotConfig.leftheld)) //if (ImGui.BeginTabItem(Strings.SlotConfig.Config))
            {

                ImGui.Spacing();
                ImGui.Indent(10);

                if (ImGui.BeginTable("Layout", 6, ImGuiTableFlags.SizingFixedFit | ImGuiTableFlags.ScrollX))
                {
                    ImGui.TableSetupColumn("labels", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("labels2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset2", ImGuiTableColumnFlags.WidthFixed);

                    ImGui.TableNextRow();
                    ImGui.TableNextColumn();
                    Rows.LeftHeld();

                    ImGui.EndTable();
                }

                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem(Strings.SlotConfig.rightheld)) //if (ImGui.BeginTabItem(Strings.SlotConfig.Config))
            {

                ImGui.Spacing();
                ImGui.Indent(10);

                if (ImGui.BeginTable("Layout", 6, ImGuiTableFlags.SizingFixedFit | ImGuiTableFlags.ScrollX))
                {
                    ImGui.TableSetupColumn("labels", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("labels2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset2", ImGuiTableColumnFlags.WidthFixed);

                    ImGui.TableNextRow();
                    ImGui.TableNextColumn();
                    Rows.RightHeld();

                    ImGui.EndTable();
                }

                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem(Strings.SlotConfig.lefttoright)) //if (ImGui.BeginTabItem(Strings.SlotConfig.Config))
            {

                ImGui.Spacing();
                ImGui.Indent(10);

                if (ImGui.BeginTable("Layout", 6, ImGuiTableFlags.SizingFixedFit | ImGuiTableFlags.ScrollX))
                {
                    ImGui.TableSetupColumn("labels", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("labels2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset2", ImGuiTableColumnFlags.WidthFixed);

                    ImGui.TableNextRow();
                    ImGui.TableNextColumn();
                    Rows.LeftToRight();

                    ImGui.EndTable();
                }

                ImGui.EndTabItem();
            }
            if (ImGui.BeginTabItem(Strings.SlotConfig.righttoleft)) //if (ImGui.BeginTabItem(Strings.SlotConfig.Config))
            {

                ImGui.Spacing();
                ImGui.Indent(10);

                if (ImGui.BeginTable("Layout", 6, ImGuiTableFlags.SizingFixedFit | ImGuiTableFlags.ScrollX))
                {
                    ImGui.TableSetupColumn("labels", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("labels2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("controls2", ImGuiTableColumnFlags.WidthFixed);
                    ImGui.TableSetupColumn("reset2", ImGuiTableColumnFlags.WidthFixed);

                    ImGui.TableNextRow();
                    ImGui.TableNextColumn();
                    Rows.RightToLeft();

                    ImGui.EndTable();
                }

                ImGui.EndTabItem();
            }

            ImGui.EndTabBar();
        }

        HudOptions.ProfileIndicator();
        ImGui.EndTabItem();
    }

    private static class Rows
    {
        public static void MainSettings()
        {
            var psConfig = Profile.PSConfig;


            // Idle

            //ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.idle.ToUpper());
            ImGui.TableNextRow();
            ImGui.TableNextColumn();
            ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.LeftHotbar);
                ImGui.TableNextColumn();
                ImGui.TableNextColumn();
                ImGui.TableNextColumn();
                ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.RightHotbar);
                ImGui.TableNextRow();
                ImGui.TableNextColumn();

            GroupSettings(Profile.Dpad_LeftOffset, "idle", "left", "dpad");
            GroupSettings(Profile.Dpad_RightOffset, "idle", "right", "dpad");
            GroupSettings(Profile.Face_LeftOffset, "idle", "left", "face", psConfig);
            GroupSettings(Profile.Face_RightOffset, "idle", "right", "face", psConfig);
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Button order is Down > Right > Up > Left // A > B > Y > X
            GroupSettings(Profile.Down_LeftOffset, "idle", "left", "down");
            GroupSettings(Profile.Down_RightOffset, "idle", "right", "down");
            GroupSettings(Profile.Right_LeftOffset, "idle", "left", "right");
            GroupSettings(Profile.Right_RightOffset, "idle", "right", "right");
            GroupSettings(Profile.Up_LeftOffset, "idle", "left", "up");
            GroupSettings(Profile.Up_RightOffset, "idle", "right", "up");
            GroupSettings(Profile.Left_LeftOffset, "idle", "left", "left");
            GroupSettings(Profile.Left_RightOffset, "idle", "right", "left");
                ImGui.TableNextRow();
                ImGui.TableNextColumn();

            GroupSettings(Profile.A_LeftOffset, "idle", "left", "a", psConfig);
            GroupSettings(Profile.A_RightOffset, "idle", "right", "a", psConfig);
            GroupSettings(Profile.B_LeftOffset, "idle", "left", "b", psConfig);
            GroupSettings(Profile.B_RightOffset, "idle", "right", "b", psConfig);
            GroupSettings(Profile.Y_LeftOffset, "idle", "left", "y", psConfig);
            GroupSettings(Profile.Y_RightOffset, "idle", "right", "y", psConfig);
            GroupSettings(Profile.X_LeftOffset, "idle", "left", "x", psConfig);
            GroupSettings(Profile.X_RightOffset, "idle", "right", "x", psConfig);

                ImGui.TableNextRow();
                ImGui.TableNextColumn();
        }

        public static void LeftHeld()
        {
            var psConfig = Profile.PSConfig;

            // Left Held

            //ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.leftheld.ToUpper());
            ImGui.TableNextRow();
            ImGui.TableNextColumn();
            ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.LeftHotbar);
                ImGui.TableNextColumn();
                ImGui.TableNextColumn();
                ImGui.TableNextColumn();
                ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.RightHotbar);
                ImGui.TableNextRow();
                ImGui.TableNextColumn();

            GroupSettings(Profile.Dpad_LeftOffset_L, "leftheld", "left", "dpad");
            GroupSettings(Profile.Dpad_RightOffset_L, "leftheld", "right", "dpad");
            GroupSettings(Profile.Face_LeftOffset_L, "leftheld", "left", "face", psConfig);
            GroupSettings(Profile.Face_RightOffset_L, "leftheld", "right", "face", psConfig);
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Button order is Down > Right > Up > Left // A > B > Y > X
            GroupSettings(Profile.Down_LeftOffset_L, "leftheld", "left", "down");
            GroupSettings(Profile.Down_RightOffset_L, "leftheld", "right", "down");
            GroupSettings(Profile.Right_LeftOffset_L, "leftheld", "left", "right");
            GroupSettings(Profile.Right_RightOffset_L, "leftheld", "right", "right");
            GroupSettings(Profile.Up_LeftOffset_L, "leftheld", "left", "up");
            GroupSettings(Profile.Up_RightOffset_L, "leftheld", "right", "up");
            GroupSettings(Profile.Left_LeftOffset_L, "leftheld", "left", "left");
            GroupSettings(Profile.Left_RightOffset_L, "leftheld", "right", "left");
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            GroupSettings(Profile.A_LeftOffset_L, "leftheld", "left", "a", psConfig);
            GroupSettings(Profile.A_RightOffset_L, "leftheld", "right", "a", psConfig);
            GroupSettings(Profile.B_LeftOffset_L, "leftheld", "left", "b", psConfig);
            GroupSettings(Profile.B_RightOffset_L, "leftheld", "right", "b", psConfig);
            GroupSettings(Profile.Y_LeftOffset_L, "leftheld", "left", "y", psConfig);
            GroupSettings(Profile.Y_RightOffset_L, "leftheld", "right", "y", psConfig);
            GroupSettings(Profile.X_LeftOffset_L, "leftheld", "left", "x", psConfig);
            GroupSettings(Profile.X_RightOffset_L, "leftheld", "right", "x", psConfig);

            ImGui.TableNextRow();
                ImGui.TableNextColumn();
        }

        public static void RightHeld()
        {
            var psConfig = Profile.PSConfig;

            // Right Held

            //ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.rightheld.ToUpper());
            ImGui.TableNextRow();
            ImGui.TableNextColumn();
            ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.LeftHotbar);
                ImGui.TableNextColumn();
                ImGui.TableNextColumn();
                ImGui.TableNextColumn();
                ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.RightHotbar);
                ImGui.TableNextRow();
                ImGui.TableNextColumn();

            GroupSettings(Profile.Dpad_LeftOffset_R, "rightheld", "left", "dpad");
            GroupSettings(Profile.Dpad_RightOffset_R, "rightheld", "right", "dpad");
            GroupSettings(Profile.Face_LeftOffset_R, "rightheld", "left", "face", psConfig);
            GroupSettings(Profile.Face_RightOffset_R, "rightheld", "right", "face", psConfig);
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Button order is Down > Right > Up > Left // A > B > Y > X
            GroupSettings(Profile.Down_LeftOffset_R, "rightheld", "left", "down");
            GroupSettings(Profile.Down_RightOffset_R, "rightheld", "right", "down");
            GroupSettings(Profile.Right_LeftOffset_R, "rightheld", "left", "right");
            GroupSettings(Profile.Right_RightOffset_R, "rightheld", "right", "right");
            GroupSettings(Profile.Up_LeftOffset_R, "rightheld", "left", "up");
            GroupSettings(Profile.Up_RightOffset_R, "rightheld", "right", "up");
            GroupSettings(Profile.Left_LeftOffset_R, "rightheld", "left", "left");
            GroupSettings(Profile.Left_RightOffset_R, "rightheld", "right", "left");
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            GroupSettings(Profile.A_LeftOffset_R, "rightheld", "left", "a", psConfig);
            GroupSettings(Profile.A_RightOffset_R, "rightheld", "right", "a", psConfig);
            GroupSettings(Profile.B_LeftOffset_R, "rightheld", "left", "b", psConfig);
            GroupSettings(Profile.B_RightOffset_R, "rightheld", "right", "b", psConfig);
            GroupSettings(Profile.Y_LeftOffset_R, "rightheld", "left", "y", psConfig);
            GroupSettings(Profile.Y_RightOffset_R, "rightheld", "right", "y", psConfig);
            GroupSettings(Profile.X_LeftOffset_R, "rightheld", "left", "x", psConfig);
            GroupSettings(Profile.X_RightOffset_R, "rightheld", "right", "x", psConfig);

            ImGui.TableNextRow();
                ImGui.TableNextColumn();
        }

        public static void LeftToRight()
        {
            var psConfig = Profile.PSConfig;

            // Left to Right

            //ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.lefttoright.ToUpper());
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Square is so silly
            GroupSettings(Profile.Face_LeftOffset_LR, "lefttoright", "left", "dpad");            // Dpad
            GroupSettings(Profile.Dpad_RightOffset_LR, "lefttoright", "left", "face", psConfig); // Face
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Button order is Down > Right > Up > Left // A > B > Y > X
            GroupSettings(Profile.Down_RightOffset_LR, "lefttoright", "right", "down");
            GroupSettings(Profile.A_LeftOffset_LR, "lefttoright", "left", "a", psConfig);
            GroupSettings(Profile.Right_RightOffset_LR, "lefttoright", "right", "right");
            GroupSettings(Profile.B_LeftOffset_LR, "lefttoright", "left", "b", psConfig);
            GroupSettings(Profile.Up_RightOffset_LR, "lefttoright", "right", "up");
            GroupSettings(Profile.Y_LeftOffset_LR, "lefttoright", "left", "y", psConfig);
            GroupSettings(Profile.Left_RightOffset_LR, "lefttoright", "right", "left");
            GroupSettings(Profile.X_LeftOffset_LR, "lefttoright", "left", "x", psConfig);

            ImGui.TableNextRow();
                ImGui.TableNextColumn();
        }

        public static void RightToLeft()
        {
            var psConfig = Profile.PSConfig;

            // Right to Left

            //ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.righttoleft.ToUpper());
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Square is so silly
            GroupSettings(Profile.Face_LeftOffset_RL, "righttoleft", "left", "dpad");            // Dpad
            GroupSettings(Profile.Dpad_RightOffset_RL, "righttoleft", "left", "face", psConfig); // Face
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Button order is Down > Right > Up > Left // A > B > Y > X
            GroupSettings(Profile.Down_RightOffset_RL, "righttoleft", "right", "down");
            GroupSettings(Profile.A_LeftOffset_RL, "righttoleft", "left", "a", psConfig);
            GroupSettings(Profile.Right_RightOffset_RL, "righttoleft", "right", "right");
            GroupSettings(Profile.B_LeftOffset_RL, "righttoleft", "left", "b", psConfig);
            GroupSettings(Profile.Up_RightOffset_RL, "righttoleft", "right", "up");
            GroupSettings(Profile.Y_LeftOffset_RL, "righttoleft", "left", "y", psConfig);
            GroupSettings(Profile.Left_RightOffset_RL, "righttoleft", "right", "left");
            GroupSettings(Profile.X_LeftOffset_RL, "righttoleft", "left", "x", psConfig);


            ImGui.TableNextRow();
                ImGui.TableNextColumn();
        }

        public static void ExpandedHoldLR()
        {
            var psConfig = Profile.PSConfig;

            // Right to Left

            //ImGui.TextColored(Helpers.DimColor, Strings.SlotConfig.righttoleft.ToUpper());
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Square is so silly
            GroupSettings(Profile.Face_RightOffset_LR, "lefttoright", "right", "dpad");            // Dpad
            GroupSettings(Profile.Dpad_LeftOffset_LR, "lefttoright", "left", "face", psConfig); // Face
            ImGui.TableNextRow();
            ImGui.TableNextColumn();

            // Button order is Down > Right > Up > Left // A > B > Y > X
            GroupSettings(Profile.Down_LeftOffset_LR, "lefttoright", "left", "down");
            GroupSettings(Profile.A_RightOffset_LR, "lefttoright", "right", "a", psConfig);
            GroupSettings(Profile.Right_LeftOffset_LR, "lefttoright", "left", "right");
            GroupSettings(Profile.B_RightOffset_LR, "lefttoright", "right", "b", psConfig);
            GroupSettings(Profile.Up_LeftOffset_LR, "lefttoright", "left", "up");
            GroupSettings(Profile.Y_RightOffset_LR, "lefttoright", "right", "y", psConfig);
            GroupSettings(Profile.Left_LeftOffset_LR, "lefttoright", "left", "left");
            GroupSettings(Profile.X_RightOffset_LR, "lefttoright", "right", "x", psConfig);


            ImGui.TableNextRow();
                ImGui.TableNextColumn();
        }

        public static void GroupSettings(Vector3 value, string status, string hotbar, string slot, bool ps = false)
        {
            var offset = new System.Numerics.Vector3(0, 0, 0); // why..?
            var reset = new System.Numerics.Vector3(0, 0, 0);
            offset = value;

            switch (slot)
            {
                case "dpad":
                    ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Dpad);
                    break;
                case "face":
                    if (ps) { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.FacePS); }
                    else { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Face); }
                    break;

                case "left":
                    ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Left_Dpad);
                    reset = new Vector3(0, 29, 0);
                    break;
                case "right":
                    ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Right_Dpad);
                    reset = new Vector3(84, 29, 0);
                    break;
                case "up":
                    ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Up_Dpad);
                    reset = new Vector3(42, 5, 0);
                    break;
                case "down":
                    ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Down_Dpad);
                    reset = new Vector3(42,53, 0);
                    break;

                case "a":
                    if (ps) { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Ex_Button); }
                    else { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.A_Button); }
                    reset = new Vector3(42, 53, 0);
                    break;
                case "b":
                    if (ps) { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Cir_Button); }
                    else { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.B_Button); }
                    reset = new Vector3(84, 29, 0);
                    break;
                case "x":
                    if (ps) { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Squ_Button); }
                    else { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.X_Button); }
                    reset = new Vector3(0, 29, 0);
                    break;
                case "y":
                    if (ps) { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Tri_Button); }
                    else { ImGui.TextColored(Helpers.HighlightColor, Strings.SlotConfig.Y_Button); }
                    reset = new Vector3(42, 5, 0);
                    break;
            }

            ImGui.TableNextColumn();

            ImGui.PushID($"Reset##{status}_{hotbar}_{slot}");
            if (ImGuiComponents.IconButton(FontAwesomeIcon.UndoAlt)) InternalCmd.SaveSlot(status, hotbar, slot, reset);
            ImGui.PopID();

            ImGui.TableNextColumn();
            ImGui.SetNextItemWidth(90 * Helpers.Scale);
            if (ImGui.DragFloat3($"##{status}_{hotbar}_{slot}", ref offset, 1, -9999, 9999, "%g")) InternalCmd.SaveSlot(status, hotbar, slot, offset);
            if (ImGui.DragFloat2($"##{status}_{hotbar}_{slot}", ref offset, 1, -9999, 9999, "%g")) InternalCmd.SaveSlot(status, hotbar, slot, offset);

            //ImGui.TableNextRow();
            ImGui.TableNextColumn();

        }
    }
}