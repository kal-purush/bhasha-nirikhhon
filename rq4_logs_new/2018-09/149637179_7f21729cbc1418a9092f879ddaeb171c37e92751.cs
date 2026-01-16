
using UnityEngine;
using UnityEditor;
using System.Collections.Generic;


public class DataFixer : MonoBehaviour
{
	[MenuItem("Tools/ReCreate Data and Prefabs")]
	static private void ReCreateDataAndPrefabs()
	{
		string[] pAllDataObjectsGUIDs = AssetDatabase.FindAssets("t:ScriptableObject t:Prefab", new string[] { "Assets/ScriptableObjects", "Assets/Prefabs" });
		for (int i = 0; i < pAllDataObjectsGUIDs.Length; ++i)
		{
			string sPath = AssetDatabase.GUIDToAssetPath(pAllDataObjectsGUIDs[i]);
			AssetDatabase.CopyAsset(sPath, sPath);
		}
	}

	[MenuItem("Tools/Reserialize all files")]
	static private void ReserializeAllFiles()
	{
		string[] pAllDataObjectsGUIDs = AssetDatabase.FindAssets("", new string[] { "Assets" });

		List<string> pDataObjectsPaths = new List<string>(pAllDataObjectsGUIDs.Length);
		for (int i = 0; i < pAllDataObjectsGUIDs.Length; ++i)
			pDataObjectsPaths.Add(AssetDatabase.GUIDToAssetPath(pAllDataObjectsGUIDs[i]));

		AssetDatabase.ForceReserializeAssets(pDataObjectsPaths, ForceReserializeAssetsOptions.ReserializeAssetsAndMetadata);
	}
}