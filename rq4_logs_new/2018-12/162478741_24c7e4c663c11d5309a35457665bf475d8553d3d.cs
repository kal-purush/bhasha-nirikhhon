#pragma warning disable 0626
#pragma warning disable 0649

/////////////////////
//// ENTRY POINT ////
/////////////////////

using System;
using ModTheGungeon;
using UnityEngine;
using UnityEngine.SceneManagement;
using System.Reflection;
using MonoMod;
using System.IO;
using System.Collections.Generic;

namespace ModUntitled.Patches {
    [MonoModPatch("global::FoyerPreloader")]
    internal class FoyerPreloader : global::FoyerPreloader {
        protected extern void orig_Awake();
        private new void Awake() {
            ModUntitled.Logger.Info("Mod Untitled entry point");
            ModUntitled.GameObject = new GameObject("Mod Untitled");

            ModUntitled.Instance = ModUntitled.GameObject.AddComponent<ModUntitled>();
            ModUntitled.Instance.GamePreload();

            orig_Awake();
        }
    }
}