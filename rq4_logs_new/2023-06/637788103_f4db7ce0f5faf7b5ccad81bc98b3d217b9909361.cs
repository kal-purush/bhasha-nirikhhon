using System;
using _Scripts.Stage;
using Unity.Mathematics;
using UnityEditor;
using UnityEngine;
using UnityEngine.Rendering.VirtualTexturing;
using Random = UnityEngine.Random;

namespace Editor
{
	[CustomEditor(typeof(SpawnPositions))]
	public class SpawnPositionsEditor : UnityEditor.Editor
	{
		private void OnSceneGUI()
		{
			SpawnPositions spawner = (SpawnPositions) target;
			
			for (int i = 0; i < spawner.spawnPositions.Count; i++)
			{
				EditorGUI.BeginChangeCheck();
				Vector3 pos = Handles.PositionHandle(spawner.spawnPositions[i] + spawner.transform.position, Quaternion.identity);
				
				if (EditorGUI.EndChangeCheck()) {
					Undo.RecordObject(spawner, "Move spawn position"); 
					EditorUtility.SetDirty(spawner);
					spawner.spawnPositions[i] = pos;
				}
			}
		}
	}
}