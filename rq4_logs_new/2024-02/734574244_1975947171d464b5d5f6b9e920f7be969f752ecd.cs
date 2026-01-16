using System;
using System.Collections.Generic;
using System.Linq;
using Suhdo.Weapons.Components;
using UnityEditor;
using UnityEditor.Callbacks;
using UnityEngine;

namespace Suhdo.Weapons
{
	[CustomEditor(typeof(WeaponDataSO))]
	public class WeaponDataSOEditor : Editor
	{
		private static List<Type> _dataCompTypes = new List<Type>();
		private WeaponDataSO _dataSO;

		private void OnEnable()
		{
			_dataSO = target as WeaponDataSO;
		}

		public override void OnInspectorGUI()
		{
			base.OnInspectorGUI();

			foreach (var data in _dataCompTypes)
			{
				if (GUILayout.Button(data.Name))
				{
					var comp = Activator.CreateInstance(data) as ComponentData;
					if (comp != null)
						_dataSO.AddData(comp);
				}
			}
		}

		[DidReloadScripts]
		private static void ReCompile()
		{
			var assemblies = AppDomain.CurrentDomain.GetAssemblies();
			var type = assemblies.SelectMany(assembly => assembly.GetTypes());
			var filterType = type.Where(
				type => type.IsSubclassOf(typeof(ComponentData)) && type.IsClass && !type.ContainsGenericParameters
				);
			_dataCompTypes = filterType.ToList();
		}
	}
}