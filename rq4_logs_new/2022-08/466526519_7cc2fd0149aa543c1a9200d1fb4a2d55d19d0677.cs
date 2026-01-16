using System.Collections;
using System.Collections.Generic;
using UnityEditor;
using UnityEngine;

namespace Modula
{
    public static class DefineSymbolsSetter
    {
        [InitializeOnLoadMethod]
        public static void AddDefineSymbols()
        {
            var buildTarget = EditorUserBuildSettings.selectedBuildTargetGroup;
            
            var symbols = PlayerSettings.GetScriptingDefineSymbolsForGroup(buildTarget);
            var symbolsHashSet = new HashSet<string>(symbols.Split(';')) { "MODULA" };

            var modifiedSymbols = string.Join(";", symbolsHashSet);
            if (symbols == modifiedSymbols) return;
            
            PlayerSettings.SetScriptingDefineSymbolsForGroup(buildTarget, modifiedSymbols);
            Debug.Log("Adding 'MODULA' to scripting defines...");
        }
    }
}