using BepInEx;
using HarmonyLib;
using SupplementaryMines.Patches;
using DunGen;

namespace SupplementaryMines.Patches
{
    [HarmonyPatch(typeof(RoundManager))]
    internal class RoundManagerPatch
    {
        public static RoundManagerPatch Instance = null;

        void Awake()
        {
            if (Instance == null)
            {
                Instance = this;
            }
        }

        [HarmonyPatch("SpawnMapObjects")]
        [HarmonyPostfix]
        static void SpawnMapObjects()
        {
            Plugin.Log("Called SpawnMapObjects");
        }
    }
}