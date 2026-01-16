using AlienRace;
using HarmonyLib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;
using Verse;

namespace SPADE
    {
#pragma warning disable IDE0051 // Remove unused private members
    /*
    [StaticConstructorOnStartup]
    static class PatchTestingGround
        {
        static PatchTestingGround()
            {
            var harmony = new Harmony("princess.test");
            Log.Message("test patching...");
            harmony.PatchAll(Assembly.GetExecutingAssembly());
            }
        }*/

    [HarmonyPatch(typeof(PawnTextureAtlas), "TryGetFrameSet")]
    static class SPADE_PawnTextureAtlas_TryGetFrameSet_Patch
        {
        static bool Prefix(ref bool __result, Pawn pawn, ref bool createdNew, PawnTextureAtlas __instance, List<PawnTextureAtlasFrameSet> ___freeFrameSets)
            {
            if (__instance.FreeCount > 0 && ___freeFrameSets.First().meshes.First().vertices.First().x / -1f != ((pawn.def as ThingDef_AlienRace)?.alienRace.generalSettings.alienPartGenerator.borderScale ?? 1f)) 
                {
                createdNew = false;
                __result = false;
                return false;
                }
            return true;
            }

        /*
        static bool Postfix(bool ret, Pawn pawn, bool createdNew, List<PawnTextureAtlasFrameSet> ___freeFrameSets)
            {
            if (createdNew)
                Log.Message("patched method called, returned " + ret);
            if (___freeFrameSets.First().meshes.First().vertices.First().x / -1f != ((pawn.def as ThingDef_AlienRace)?.alienRace.generalSettings.alienPartGenerator.borderScale ?? 1f) && ret)
                Log.Message("Pawn atlas reuse detected!");
            return ret;
            }
        */

        /*
        static IEnumerable<CodeInstruction> Transpiler(IEnumerable<CodeInstruction> instructions)
            {
            foreach (CodeInstruction instruction in instructions)
                {
                if (instruction.opcode == OpCodes.Call
                    && instruction.operand is MethodInfo mt
                    && mt.Name.Contains("Pop"))
                    {
                    yield return new CodeInstruction(OpCodes.Ldarg_1);
                    yield return CodeInstruction.Call(typeof(SPADE_PawnTextureAtlas_TryGetFrameSet_Patch), nameof(getFrameSetForPawn));
                    }
                else
                    yield return instruction;
                }
            }
        */

        /*
        public static PawnTextureAtlasFrameSet getFrameSetForPawn(List<PawnTextureAtlasFrameSet> list, Pawn pawn) 
            {
            // shoutout to the 'goes to' operator
            // for (int i = list.Count - 1; i > 0; i--)
            // iterating backwards because you know, vanilla compat and such
            // also 99% of the time the element is at the tail cause it was just pushed
            for (int i = list.Count; i --> 0;)
                {
                if (list[i].meshes.First().vertices.First().x / -1f == ((pawn.def as ThingDef_AlienRace)?.alienRace.generalSettings.alienPartGenerator.borderScale ?? 1f)) 
                    {
                    PawnTextureAtlasFrameSet result = list[i];
                    list.RemoveAt(i);
                    return result;
                    }
                }

            return list.Pop();
            }
        */
        }
    }