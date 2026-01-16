// Copyright (C) 2023 Nicholas Maltbie
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
// associated documentation files (the "Software"), to deal in the Software without restriction,
// including without limitation the rights to use, copy, modify, merge, publish, distribute,
// sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
// BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

using nickmaltbie.Treachery.Interactive.Stamina;
using Unity.Netcode;
using UnityEditor;
using UnityEngine;

namespace nickmaltbie.Treachery.DebugScripts
{
    [CustomEditor(typeof(StaminaMeter))]
    public class StaminaMeterDebug : Editor
    {
        public override void OnInspectorGUI()
        {
            DrawDefaultInspector();

            var stamina = target as StaminaMeter;

            GUI.enabled = false;
            EditorGUILayout.FloatField("Remaining Stamina", stamina.RemainingStamina);
            EditorGUILayout.FloatField("Maximum Stamina", stamina.MaximumStamina);
            GUI.enabled = true;

            if (NetworkManager.Singleton?.IsServer ?? false && stamina.GetComponent<NetworkObject>().IsSpawned)
            {
                if (GUILayout.Button("Spend 10 Stamina"))
                {
                    stamina.SpendStamina(10);
                }

                if (GUILayout.Button("Restore 10 Stamina"))
                {
                    stamina.RestoreStamina(10);
                }
            }
        }
    }
}