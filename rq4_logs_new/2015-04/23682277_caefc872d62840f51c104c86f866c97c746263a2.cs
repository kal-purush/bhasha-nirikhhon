using UnityEngine;
using UnityEditor;

public class BatchEdit : ScriptableObject {

	[MenuItem("GameObject/Set Shader to Standard")]
	static void GetSelection()
	{
		Shader standardShader = Shader.Find("Standard");

		if( standardShader == null )
			return;

		foreach( GameObject selectionObj in Selection.gameObjects )
		{
			MeshRenderer meshRenderer = selectionObj.GetComponent<MeshRenderer>();

			if( meshRenderer )
			{
				Material[] materials = meshRenderer.sharedMaterials;

				for( int i = 0; i < materials.Length; i++ )
					materials[i].shader = standardShader;
			}

			foreach( MeshRenderer childRenderer in selectionObj.GetComponentsInChildren<MeshRenderer>() )
			{
				MeshRenderer childMeshRenderer = selectionObj.GetComponent<MeshRenderer>();
				
				if( childMeshRenderer )
				{
					Material[] childMaterials = childMeshRenderer.sharedMaterials;
					
					for( int i = 0; i < childMaterials.Length; i++ )
						childMaterials[i].shader = standardShader;
				}
			}
		}
	}
}