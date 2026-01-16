using UnityEditor;
using UnityEngine;

namespace Editor
{
	[CustomEditor(typeof(Finish))]
	public class FinishEditor : UnityEditor.Editor
	{
		private void OnSceneGUI()
		{
			Finish finish = (Finish) target;

			for (int i = 0; i < finish.positions.Length; i++)
			{
				EditorGUI.BeginChangeCheck();
				Vector3 pos = Handles.PositionHandle(finish.positions[i], Quaternion.identity);
				
				if (EditorGUI.EndChangeCheck()) {
					Undo.RecordObject(finish, "Move monster spawn position"); 
					EditorUtility.SetDirty(finish);
					finish.positions[i] = pos;
				}
			}
		}
	}
}