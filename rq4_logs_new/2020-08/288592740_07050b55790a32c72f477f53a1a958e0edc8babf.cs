using System;
using System.Collections.Generic;
using System.Linq;
using Mutagen.Bethesda;
using Mutagen.Bethesda.Synthesis;
using Mutagen.Bethesda.Skyrim;
using Noggog;

namespace KhajiitEarsShow
{
    public class Program
    {
        public static int Main(string[] args)
        {
            return SynthesisPipeline.Instance.Patch<ISkyrimMod, ISkyrimModGetter>(
                args: args,
                patcher: RunPatch,
                new UserPreferences()
                {
                    ActionsForEmptyArgs = new RunDefaultPatcher()
                    {
                        IdentifyingModKey = "KhajiitEarsShow.esp",
                        TargetRelease = GameRelease.SkyrimSE
                    }
                }
            );
        }

        public static void RunPatch(SynthesisState<ISkyrimMod, ISkyrimModGetter> state)
        {
            FormKey khajiitKey = new FormKey("Skyrim.esm", 0x013745);

            foreach (var armorAddon in state.LoadOrder.PriorityOrder.WinningOverrides<IArmorAddonGetter>())
            {
                if (!armorAddon.Race.FormKey.Equals(khajiitKey)) continue;

                if (!armorAddon.BodyTemplate.FirstPersonFlags.HasFlag(BipedObjectFlag.Ears)) continue;

                var modifiedArmorAddon = state.PatchMod.ArmorAddons.GetOrAddAsOverride(armorAddon);
                modifiedArmorAddon.BodyTemplate.FirstPersonFlags &= ~BipedObjectFlag.Ears;
            }
        }
    }
}