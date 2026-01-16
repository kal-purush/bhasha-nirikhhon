using Godot;
using System;
using System.Collections.Generic;

public static class MeshManager {
	
	private static Dictionary<string, Mesh> avialableMeshes = new Dictionary<string, Mesh>();
	
	private static Mesh defaultMesh = null;
	
	//Methods
	
	private static Mesh CreateDefaultMesh()
	{
		var st = new SurfaceTool();

		st.Begin(Mesh.PrimitiveType.Triangles);
			
		st.AddVertex(new Vector3(-1, -1, 0));
			
		st.AddVertex(new Vector3(-1, 1, 0));
			
		st.AddVertex(new Vector3(1, 1, 0));
			
		st.AddVertex(new Vector3(-1, -1, 0));
			
		st.AddVertex(new Vector3(1, -1, 0));
			
		st.AddVertex(new Vector3(1, 1, 0));

		// And finally, generate the mesh.
		var meshBuffer = st.Commit();
		defaultMesh = meshBuffer;
		
		return defaultMesh;
	}
	
	public static Mesh FetchDefaultMesh()
	{
		if (defaultMesh == null)
			defaultMesh = CreateDefaultMesh();
		return defaultMesh; 
	}
	
	public static Mesh FetchMesh(string name)
	{
		//not for now : )
		return null;
	}

}
