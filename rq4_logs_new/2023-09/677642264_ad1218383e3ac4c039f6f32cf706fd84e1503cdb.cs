using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEditor;
using UnityEditor.UI;
using UnityEngine;
using UnityEngine.UI;

namespace Flappy.UI
{
	// RaycastTargetとして機能させるだけのクラス
	public class Modal : Image
	{
		public bool IsDrawing = true;

		protected override void OnPopulateMesh(VertexHelper toFill)
		{
			if( this.IsDrawing == true )
			{
				base.OnPopulateMesh(toFill);
				return;
			}

			toFill.Clear();
		}
	}

#if UNITY_EDITOR
	[CanEditMultipleObjects, CustomEditor(typeof(Modal), true)]
	public class ModalEditor : ImageEditor
	{
		public override void OnInspectorGUI()
		{
			base.OnInspectorGUI();
			this.serializedObject.Update();
			EditorGUILayout.PropertyField(this.serializedObject.FindProperty("IsDrawing"), true);
			this.serializedObject.ApplyModifiedProperties();
		}
	}
#endif
}