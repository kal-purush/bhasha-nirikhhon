using UnityEngine;
using System.Collections;
using System.Collections.Generic;

public class SceneGridManager : MonoBehaviour {
	
	public float squareSide;
	public int minX, maxX, minY, maxY;
	public int[,] CostToTraverse;
	
	internal struct GridData
	{
		public int x, y;
		public int cost;
	};
	
	private int xBias = 0, yBias = 0;
	private int gridMaxX, gridMaxY;
	private IDictionary<int, GridData> objectToGridData;
	// Use this for initialization
	void Start ()
	{
		if (minX < 0) {
			xBias = 1 - minX;
		}
		if (minY < 0) {
			yBias = 1 - minY;
		}
		
		// Convert coordinates to positives
		gridMaxX = maxX + xBias;
		gridMaxY = maxY + yBias;
		CostToTraverse = new int[gridMaxX, gridMaxY];
		objectToGridData = new Dictionary<int, GridData>();
	}
	
	// Update is called once per frame
	void Update () {
	
	}
	
	public void getGridCoords(float worldX, float worldY, out int gridX, out int gridY)
	{
		gridX = (int)((worldX + xBias) / gridMaxX);
		gridY = (int)((worldY + yBias) / gridMaxY);
	}
	
	public void registerObject(GameObject obj, int ncost)
	{
		GridData d = new GridData();
		d.cost = ncost;
		getGridCoords(obj.transform.position.x, obj.transform.position.y, out d.x, out d.y);
		objectToGridData[obj.GetHashCode()] = d;
	}
	
	public void updateObjectPosition(GameObject obj)
	{
		GridData data = objectToGridData[obj.GetHashCode()];
		CostToTraverse[data.x, data.y] -= data.cost;
		getGridCoords(obj.transform.position.x, obj.transform.position.y, out data.x, out data.y);
		objectToGridData[obj.GetHashCode()] = data;
	}
}