using UnityEditor;
using UnityEngine;

namespace GB_Platformer
{
    [CustomEditor(typeof(GameRoot))]
    public class GameRootCustomEditor : Editor
    {
        public override void OnInspectorGUI()
        {
            if (GUILayout.Button("GetCoins"))
            {
                GameRoot gameRoot = (GameRoot)target;
                gameRoot.GameData.FindCoinsInScene();
            }
            DrawDefaultInspector();
        }
    }
}