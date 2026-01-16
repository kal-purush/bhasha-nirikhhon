using HarmonyLib;
using Verse.AI;

namespace ThrowThem;

[HarmonyPatch(typeof(JobDriver_AttackStatic), "MakeNewToils")]
internal class AttackStaticPatch
{
    public static void Prefix(JobDriver_AttackStatic __instance)
    {
        var pawn = __instance.pawn;
        var curJob = pawn.CurJob;
        var verbToUse = curJob.verbToUse;
        if (verbToUse?.EquipmentSource != null)
        {
            __instance.FailOnMissingItems(pawn, pawn.CurJob.verbToUse.EquipmentSource);
        }
    }
}