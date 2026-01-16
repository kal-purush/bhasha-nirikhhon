using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Random = UnityEngine.Random;


public class BoardManager : MonoBehaviour 
{
	[SerializeField]
	public class Count {
		public int minimum;
		public int maximum;

		public Count(int min, int max) 
		{
			minimum = min;
			maximum = max;
		}
	}

	// board size
	public int columns = 16;
	public int rows = 16;
	// number of buildings and zombies
	public Count blockcount = new Count (5, 10);    // buildings cannot enter
	public Count buildingcount = new Count (5, 10); // buildings can enter
	public Count zombiecount = new Count (3, 20);
	public GameObject exit;
	public GameObject[] floorTiles;
	public GameObject[] blockTiles;
	public GameObject[] buildTiles;
	public GameObject[] enemyTiles;

	private Transform boardHolder;
	private List <Vector3> gridPositions = new List<Vector3>();   

	// clear the grid positions 
	void InitializeList() 
	{
		gridPositions.Clear();
		for (int x = 1; x < columns - 1; x++) 
		{
			for (int y = 1; y < rows - 1; y++)
			{
				gridPositions.Add(new Vector3(x,y,0f));
			}
		}
		
	}

	// set up the floor tiles
	void BoardSetup()
	{
		boardHolder = new GameObject ("Board").transform;

		for (int x = -1; x < columns + 1; x++) 
		{
			for (int y = -1; y < rows + 1; y++)
			{
				GameObject toInstantiate = floorTiles[Random.Range (0, floorTiles.Length)];

				GameObject instance = Instantiate (toInstantiate, new Vector3 (x, y, 0f), Quaternion.identity) as GameObject;

				instance.transform.SetParent (boardHolder);
			}
		}
	}

	Vector3 RandomPosition()
	{
		int randomIndex = Random.Range (0, gridPositions.Count);
		Vector3 randomPosition = gridPositions [randomIndex];

		// to avoid spawn object in the same location, remove that position after generated
		gridPositions.RemoveAt(randomIndex);
		return randomPosition;
	}

	// generate buildings
	void LayoutObjectAtRamdom(GameObject[] tileArray, int minimum, int maximum)
	{
		int objectCount = Random.Range (minimum, maximum + 1);

		for (int i = 0; i < objectCount; i++)
		{
			Vector3 randomPosition = RandomPosition();
			GameObject tileChoice = tileArray[Random.Range (0, tileArray.Length)];
			Instantiate (tileChoice, randomPosition, Quaternion.identity);
		}
	}

	public void SetupScene(int level)
	{
		BoardSetup ();
		InitializeList ();
		LayoutObjectAtRamdom (buildTiles, buildingcount.minimum, buildingcount.maximum);
		LayoutObjectAtRamdom (blockTiles, blockcount.minimum, blockcount.maximum);

		// generate enemy number by the level
		int enemyCount = (int)Mathf.Log (level, 2f);
		LayoutObjectAtRamdom(enemyTiles, enemyCount, enemyCount);

		Instantiate (exit, new Vector3 (columns / 2, rows - 1, 0F), Quaternion.identity);

	}

}