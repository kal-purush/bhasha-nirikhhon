using UnityEngine;
using System.Collections;

public class generateMaze : MonoBehaviour {
	public int mazeWidth_Y = 50;
	public int mazeLength_X = 50;
	public float spacing = 0.96f;

	public GameObject tile_001;
	public GameObject tile_002;
	public GameObject tile_003;
	public GameObject tile_004;
	public GameObject tile_005;
	public GameObject tile_006;
	public GameObject tile_007;
	public GameObject tile_008;
	public GameObject tile_009;
	public GameObject tile_010;
	public GameObject tile_011;
	public GameObject tile_012;
	public GameObject tile_013;
	public GameObject tile_014;
	public GameObject tile_015;
	public GameObject tile_016;
    public GameObject tile_017;
    public GameObject tile_018;
    public GameObject tile_019;

    void Start () {
		GameObject maze = new GameObject("Maze");
		GameObject innerTiles = new GameObject("Inner Tiles");
		GameObject walls = new GameObject("Walls");
		GameObject northWall = new GameObject("North Wall");
		GameObject southWall = new GameObject("South Wall");
		GameObject eastWall = new GameObject("East Wall");
		GameObject westWall = new GameObject("West Wall");

		walls.transform.parent = maze.transform;
		innerTiles.transform.parent = maze.transform;
		northWall.transform.parent = walls.transform;
		southWall.transform.parent = walls.transform;
		eastWall.transform.parent = walls.transform;
		westWall.transform.parent = walls.transform;

		for (int y = 0; y < mazeWidth_Y; y++) {
			for (int x = 0; x < mazeLength_X; x++) {
				int rand = Random.Range (0, 18);
				GameObject tile = tile_001;
				//int z_rotation = 0;

				if (rand == 0) { tile = tile_001; }
				else if (rand == 1) { tile = tile_002; }
				else if (rand == 2) { tile = tile_003; }
				else if (rand == 3) { tile = tile_004; }
				else if (rand == 4) { tile = tile_005; }
				else if (rand == 5) { tile = tile_006; }
				else if (rand == 6) { tile = tile_007; }
				else if (rand == 7) { tile = tile_008; }
				else if (rand == 8) { tile = tile_009; }
				else if (rand == 9) { tile = tile_010; }
				else if (rand == 10) { tile = tile_011; }
				else if (rand == 11) { tile = tile_012; }
				else if (rand == 12) { tile = tile_013; }
				else if (rand == 13) { tile = tile_014; }
				else if (rand == 14) { tile = tile_015; }
				else if (rand == 15) { tile = tile_016; }
                else if (rand == 16) { tile = tile_017; }
                else if (rand == 17) { tile = tile_018; }
                else if (rand == 18) { tile = tile_019; }

                /*rand = Random.Range (0, 3);
				if (rand == 0){ z_rotation = 0; }
				if (rand == 1){ z_rotation = 90; }
				if (rand == 2){ z_rotation = 180; }
				if (rand == 3){ z_rotation = 270; }*/

                GameObject innerTile = Instantiate(tile, new Vector3((x * spacing), (y * spacing), 0), Quaternion.identity) as GameObject;
				innerTile.transform.parent = innerTiles.transform;
			}
		}

		for (int x = 0; x < mazeLength_X; x++) {
			GameObject tile = tile_003;
			GameObject northWallTile = Instantiate(tile, new Vector3((x * spacing), (mazeWidth_Y * spacing), 0), Quaternion.identity) as GameObject; // North Wall

            tile = tile_008;
            GameObject southWallTile = Instantiate(tile, new Vector3((x * spacing), -spacing, 0), Quaternion.identity) as GameObject; // South Wall


            northWallTile.transform.parent = northWall.transform;
			southWallTile.transform.parent = southWall.transform;
		}

		for (int y = 0; y < mazeWidth_Y; y++) {
			GameObject tile = tile_010;
			GameObject eastWallTile = Instantiate(tile, new Vector3((mazeLength_X * spacing), (y * spacing), 0), Quaternion.identity) as GameObject; // East Wall

            tile = tile_006;
            GameObject westWallTile = Instantiate(tile, new Vector3(-spacing, (y * spacing), 0), Quaternion.identity) as GameObject; // West Wall

            eastWallTile.transform.parent = eastWall.transform;
			westWallTile.transform.parent = westWall.transform;
		}
	}
}