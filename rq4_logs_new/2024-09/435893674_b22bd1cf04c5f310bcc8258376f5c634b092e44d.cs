
using HarmonyLib;
using Microsoft.Xna.Framework;
using stardew_access.Features;
using StardewValley;
using StardewValley.Menus;

namespace stardew_access.Patches;

internal class MouseOverrideForTileViewer : IPatch
{
    private static Vector2? _mousePosition = null;
    internal static Vector2? MousePosition
    {
        get { return _mousePosition; }
        set
        {
            if (value.HasValue)
            {
                // Clamp the X value between 0 and Game1.viewport.Width
                float clampedX = Math.Clamp(value.Value.X, 0, Game1.viewport.Width - 1);

                // Clamp the Y value between 0 and Game1.viewport.Height
                float clampedY = Math.Clamp(value.Value.Y, 0, Game1.viewport.Height - 1);

                // Update the _mousePosition with the clamped values
                _mousePosition = new Vector2(clampedX, clampedY);
            }
            else
            {
                _mousePosition = null; // Allow setting to null if needed
            }
        }
    }

    public void Apply(Harmony harmony)
    {
        harmony.Patch(
                original: AccessTools.DeclaredMethod(typeof(CarpenterMenu), "update"),
                prefix: new HarmonyMethod(typeof(MouseOverrideForTileViewer), nameof(MouseOverrideForTileViewer.UpdatePatch))
        );

        harmony.Patch(
                original: AccessTools.DeclaredMethod(typeof(PurchaseAnimalsMenu), "update"),
                prefix: new HarmonyMethod(typeof(MouseOverrideForTileViewer), nameof(MouseOverrideForTileViewer.UpdatePatch))
        );

        harmony.Patch(
                original: AccessTools.DeclaredMethod(typeof(CarpenterMenu), "update"),
                prefix: new HarmonyMethod(typeof(MouseOverrideForTileViewer), nameof(MouseOverrideForTileViewer.UpdatePatch))
        );

        harmony.Patch(
                original: AccessTools.DeclaredMethod(typeof(CarpenterMenu), "draw"),
                prefix: new HarmonyMethod(typeof(MouseOverrideForTileViewer), nameof(MouseOverrideForTileViewer.DrawPatch))
        );

        harmony.Patch(
                original: AccessTools.DeclaredMethod(typeof(PurchaseAnimalsMenu), "draw"),
                prefix: new HarmonyMethod(typeof(MouseOverrideForTileViewer), nameof(MouseOverrideForTileViewer.DrawPatch))
        );

        harmony.Patch(
                original: AccessTools.DeclaredMethod(typeof(AnimalQueryMenu), "draw"),
                prefix: new HarmonyMethod(typeof(MouseOverrideForTileViewer), nameof(MouseOverrideForTileViewer.DrawPatch))
        );
    }

    
    /// <summary>
    /// Prevents mouse from auto panning when at edge (in builder viewport)
    /// </summary>
    internal static bool UpdatePatch(CarpenterMenu __instance)
    {
        try
        {
            if (!TileViewer.IsInMenuBuilderViewport()) return true;

            // Taken from game's source code [CarpenterMenu::update]
            int num = Game1.getOldMouseX(ui_scale: false) + Game1.viewport.X;
            int num2 = Game1.getOldMouseY(ui_scale: false) + Game1.viewport.Y;

            if (num - Game1.viewport.X < 64)
            {
                return false;
            }
            else if (num - (Game1.viewport.X + Game1.viewport.Width) >= -128) // Constant value changed from original!!
            {
                return false;
            }
            if (num2 - Game1.viewport.Y < 64)
            {
                return false;
            }
            else if (num2 - (Game1.viewport.Y + Game1.viewport.Height) >= -64)
            {
                return false;
            }
        }
        catch (Exception e)
        {
            Log.Error($"[CarpenterMenuPatch::UpdatePatch] {e.Message}:\n{e.StackTrace}");
        }

        return true;
    }

    /// <summary>
    /// Updates mouse position when in builder viewport
    /// </summary>
    /// <param name="__instance">[TODO:description]</param>
    internal static void DrawPatch(IClickableMenu __instance)
    {
        try
        {
            if (!TileViewer.IsInMenuBuilderViewport())
            {
                MousePosition = null;
                return;
            }

            if (MousePosition == null)
            {
                // var location = carpenterMenu.GetInitialBuildingPlacementViewport(carpenterMenu.TargetLocation);
                MousePosition = new((Game1.viewport.Width / 2f) - (Game1.tileSize / 2), (Game1.viewport.Height / 2f) - (Game1.tileSize / 2));
            }
            if (MousePosition != null && MousePosition.HasValue) Game1.setMousePosition((int)MousePosition.Value.X, (int)MousePosition.Value.Y);
        }
        catch (Exception e)
        {
            Log.Error($"[CarpenterMenuPatch::MouseOverride] {e.Message}:\n{e.StackTrace}");
        }
    }
}