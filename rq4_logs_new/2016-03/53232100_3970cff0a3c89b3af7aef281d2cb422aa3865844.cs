using UnityEngine;
using UnityEditor;
using System.Collections;

[CustomEditor (typeof(Tiles))]
public class TilesEditor : Editor
{
	Tiles tileMgr;

	//last aligned pos, we'll use it to _NOT_ create multiple tiles in the same place
	bool lastPosSet = false;
	Vector3 lastPos;

	//have any tiles been moved? We'll use that for Undo
	bool moved = false;

	//have tiles been painted? We'll use that for Undo
	bool painted = false;

	//have tiles been deleted? We'll use that for Undo
	bool deleted = false;

	void OnEnable()
	{
		tileMgr = (Tiles)target;
		SceneView.onSceneGUIDelegate += UpdateTiles;
	}

	void UpdateTiles(SceneView sceneview)
	{
		//get the current event
		Event e = Event.current;

		//if the toggle key is set and was pressed, change then toggle activation
		if ((tileMgr.disableKey != "") && (e.isKey && e.character == tileMgr.disableKey[0]))
			tileMgr.enabled = false;

		//if disabled, return
		if (tileMgr.enabled == false)
			return;

		//calculating the world space mouse position in the scene view
		Vector3 tmp = new Vector3(e.mousePosition.x, -e.mousePosition.y + Camera.current.pixelHeight);
		Ray r = Camera.current.ScreenPointToRay(tmp);
		Vector3 mousePos = new Vector3(r.origin.x, r.origin.y, tileMgr.depth);

		//aligning to grid
		Vector3 aligned = new Vector3(
			Mathf.Floor((mousePos.x - tileMgr.offsetX)/tileMgr.width)*tileMgr.width + tileMgr.width/2.0f + (tileMgr.offsetX) + tileMgr.objOffsetX,
			Mathf.Floor((mousePos.y - tileMgr.offsetY)/tileMgr.height)*tileMgr.height + tileMgr.height/2.0f + (tileMgr.offsetY) + tileMgr.objOffsetY,
			tileMgr.depth);

		//if drawKey is set then draw the aligned tile
		if ((tileMgr.setParentKey != "") && (e.isKey && e.character == tileMgr.setParentKey[0]))
		{
			if (Selection.activeObject != null)
				tileMgr.parent = (Transform)Selection.activeObject;
		}
		else if ((tileMgr.drawKey != "") && (e.isKey && e.character == tileMgr.drawKey[0]))
		{	
			if ((lastPos == aligned) && lastPosSet)
				return;

			if (Selection.activeObject == null)
				return;

			if (!painted)
			{
				Undo.IncrementCurrentGroup(); //Create undo state for the tile painting
				painted = true;		
			}

			//if there's already a tile, delete it
			if (tileMgr.parent != null)
			{
				foreach (Transform child in tileMgr.parent)
				{
					//we don't care about depth while deleting
					if (child.position.x == aligned.x && child.position.y == aligned.y)
					{
						DestroyImmediate(child.gameObject);
						break;
					}
				}
			}

			//our new tile
			GameObject obj;

			//get the tile's prefab
			Object prefab = PrefabUtility.GetPrefabParent(Selection.activeObject);

			//if prefab exists, create from prefab, if not the simply clone
			if (prefab)
			{
				obj = (GameObject)PrefabUtility.InstantiatePrefab(prefab);
				obj.transform.localScale = Selection.activeTransform.localScale;
				obj.transform.eulerAngles = Selection.activeTransform.eulerAngles;
			}
			else
				obj = (GameObject)Instantiate(Selection.activeObject);

			Undo.RegisterCreatedObjectUndo(obj, "Create " + obj.name);
			//set the tile's position and parent
			obj.transform.position = aligned;
			obj.transform.parent = tileMgr.parent;

			//Debug.Log("Created Tile at " + aligned); //logging where the tile was placed in world space
			lastPos = aligned;
			lastPosSet = true;
		}
		else if ((tileMgr.deleteKey != "") && (e.isKey && e.character == tileMgr.deleteKey[0]))
		{
			//we delete only inside the parent object, it could be a mess to use hierarchy for that
			if (tileMgr.parent != null)
			{
				if (!deleted)
				{
					Undo.IncrementCurrentGroup();
					Undo.RegisterSceneUndo("Delete Tiles");
					deleted = true;
				}

				foreach (Transform child in tileMgr.parent)
				{
					//we don't care about depth while deleting
					if (child.position.x == aligned.x && child.position.y == aligned.y) 
					{
						DestroyImmediate(child.gameObject);
						break;
					}
				}

			}
		}
		else if ((tileMgr.alignKey != "") && (e.isKey && e.character == tileMgr.alignKey[0]))
		{	
			//we align only if there are selected objects
			if (Selection.transforms.Length > 0)
			{
				if (!moved)
				{
					Undo.IncrementCurrentGroup();
					Undo.RegisterSceneUndo("Move Tiles");
					moved = true;
				}

				Vector3 posOffset = aligned - Selection.transforms[0].position;
				Selection.transforms[0].position = aligned;

				for (int i = 1; i < Selection.transforms.Length; ++i)
					Selection.transforms[i].position += posOffset;
			}
		}
		else if ((tileMgr.incDepthKey != "") && (e.isKey && e.character == tileMgr.incDepthKey[0]))
		{
			tileMgr.depth += 1.0f;
			Debug.Log("Now placing tiles at Z equal to " + tileMgr.depth);
		}
		else if ((tileMgr.decDepthKey != "") && (e.isKey && e.character == tileMgr.decDepthKey[0]))
		{
			tileMgr.depth -= 1.0f;
			Debug.Log("Now placing tiles at Z equal to " + tileMgr.depth);
		}
		else if (e.type == EventType.KeyUp)
		{
			//if the key is up and we were either moving, painting or deleting, the we se the appropiate variables
			//to their appropiate values
			if (moved && tileMgr.parent)
			{
				moved = false;

				//we need to delete the tiles that has been replaced
				for (int i = 0; i < Selection.transforms.Length; ++i)
				{
					foreach (Transform child in tileMgr.parent)
					{
						if (Selection.transforms[i] == child)
							continue;

						if (Selection.transforms[i].position == child.position)
						{
							DestroyImmediate(child.gameObject);
							break;
						}
					}
				}
			}
			else if (painted)
			{
				painted = false;
			}
			else if (deleted)
				deleted = false;
		}

	}

	//just a couple of simple fields
	public override void OnInspectorGUI()
	{	
		GUILayout.BeginHorizontal();
		GUILayout.Label(" Tile Width ");
		tileMgr.width = EditorGUILayout.FloatField(tileMgr.width, GUILayout.Width(50));
		GUILayout.Label(" Tile Height ");
		tileMgr.height = EditorGUILayout.FloatField(tileMgr.height, GUILayout.Width(50));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Grid Offset X ");
		tileMgr.offsetX = EditorGUILayout.FloatField(tileMgr.offsetX, GUILayout.Width(40));
		GUILayout.Label(" Grid Offset Y ");
		tileMgr.offsetY = EditorGUILayout.FloatField(tileMgr.offsetY, GUILayout.Width(40));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Object Offset X ");
		tileMgr.objOffsetX = EditorGUILayout.FloatField(tileMgr.objOffsetX, GUILayout.Width(30));
		GUILayout.Label(" Object Offset Y ");
		tileMgr.objOffsetY = EditorGUILayout.FloatField(tileMgr.objOffsetY, GUILayout.Width(30));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Grid Color ");
		tileMgr.color = EditorGUILayout.ColorField(tileMgr.color, GUILayout.Width(150));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Z Position ");
		tileMgr.depth = EditorGUILayout.FloatField(tileMgr.depth, GUILayout.Width(100));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Draw Key ");
		tileMgr.drawKey = EditorGUILayout.TextField(tileMgr.drawKey, GUILayout.Width(100));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Delete Key ");
		tileMgr.deleteKey = EditorGUILayout.TextField(tileMgr.deleteKey, GUILayout.Width(100));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Disable Key ");
		tileMgr.disableKey = EditorGUILayout.TextField(tileMgr.disableKey, GUILayout.Width(100));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Align Key ");
		tileMgr.alignKey = EditorGUILayout.TextField(tileMgr.alignKey, GUILayout.Width(100));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Increase Depth Key ");
		tileMgr.incDepthKey = EditorGUILayout.TextField(tileMgr.incDepthKey, GUILayout.Width(100));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Decrease Depth Key ");
		tileMgr.decDepthKey = EditorGUILayout.TextField(tileMgr.decDepthKey, GUILayout.Width(100));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Is Enabled? ");
		tileMgr.enabled = EditorGUILayout.Toggle(tileMgr.enabled, GUILayout.Width(100));
		GUILayout.EndHorizontal();

		GUILayout.BeginHorizontal();
		GUILayout.Label(" Tiles' Parent ");
		tileMgr.parent = (Transform)EditorGUILayout.ObjectField(tileMgr.parent, typeof(Transform), true, GUILayout.Width(100));
		GUILayout.EndHorizontal();


		//button to align the grid to the tiles' parent
		if (GUILayout.Button("Align Grid Offset With Parent", GUILayout.Width(255)))
		{	
			tileMgr.offsetX = tileMgr.parent.position.x;
			tileMgr.offsetY = tileMgr.parent.position.y;
		}

		//button to align the grid to the tiles' parent and offset it by 0.5 to deter renderer's .f point errors
		if (GUILayout.Button("Align Grid Offset With Parent + 0.5", GUILayout.Width(255)))
		{	
			tileMgr.offsetX = Mathf.Floor(tileMgr.parent.position.x) + 0.5f;
			tileMgr.offsetY = Mathf.Floor(tileMgr.parent.position.y) + 0.5f;
		}

		//repaint the scene to see the changes immediately
		SceneView.RepaintAll();
	}
}