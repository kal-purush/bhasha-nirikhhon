using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Tilemaps;


public class mapGen : MonoBehaviour
{
	
	[SerializeField] private Tilemap floorTiles;
	[SerializeField] private Tilemap wallTiles;
	[SerializeField] private Tilemap BouncyWallTiles;
	[SerializeField] private Tile floorTile;
	[SerializeField] private Tile wallTile;
	[SerializeField] private Tile BouncyWallTile;

	enum LevelTile {empty, floor, wall, bouncyWall}; 
    LevelTile[,] grid;
	Tilemap tilemap;
	struct RandomWalker {
		public Vector2 dir;
		public Vector2 pos;
	}
	List<RandomWalker> walkers; 
	
	//why has the mans got all this public...
	/*
	public GameObject[] floorTiles;
    public GameObject[] wallTiles;
    public GameObject[] bottomWallTiles;
    public GameObject exit;
    public GameObject player;

    public GameObject virtualCamera;
	*/

    public int levelWidth;
    public int levelHeight;
	public float percentToFill = 0.2f; 
	public float chanceWalkerChangeDir = 0.5f;
    public float chanceWalkerSpawn = 0.05f;
    public float chanceWalkerDestoy = 0.05f;
    public int maxWalkers = 10;
    public int iterationSteps = 100000;
	
	//        Debug.Log("LOG EXAMPLE" + someVar);

	
	void Awake(){
		Setup();
		CreateFloors();    
        CreateWalls();
		//CreateBottomWalls();
		SpawnLevel();
	
	}
	
	void Setup() {
		//clear tileMaps
		wallTiles.ClearAllTiles(); 
		floorTiles.ClearAllTiles(); 
		
        // prepare grid
		grid = new LevelTile[levelWidth, levelHeight];
		for (int x = 0; x < levelWidth - 1; x++){
			for (int y = 0; y < levelHeight - 1; y++){ 
				grid[x, y] = LevelTile.empty;
			}
		}

        //generate first walker
		walkers = new List<RandomWalker>();
		RandomWalker walker = new RandomWalker();
		walker.dir = RandomDirection();
		Vector2 pos = new Vector2(Mathf.RoundToInt(levelWidth/ 2.0f), Mathf.RoundToInt(levelHeight/ 2.0f));
		walker.pos = pos;
		walkers.Add(walker);
	}
	
	void CreateFloors() {
		int iterations = 0;
		do{
			//create floor at position of every Walker
			foreach (RandomWalker walker in walkers){
				grid[(int)walker.pos.x,(int)walker.pos.y] = LevelTile.floor;
			}

			//chance: destroy Walker
			int numberChecks = walkers.Count;
			for (int i = 0; i < numberChecks; i++) {
				if (Random.value < chanceWalkerDestoy && walkers.Count > 1){
					walkers.RemoveAt(i);
					break;
				}
			}

			//chance: Walker pick new direction
			for (int i = 0; i < walkers.Count; i++) {
				if (Random.value < chanceWalkerChangeDir){
					RandomWalker thisWalker = walkers[i];
					thisWalker.dir = RandomDirection();
					walkers[i] = thisWalker;
				}
			}

			//chance: spawn new Walker
			numberChecks = walkers.Count;
			for (int i = 0; i < numberChecks; i++){
				if (Random.value < chanceWalkerSpawn && walkers.Count < maxWalkers) {
					RandomWalker walker = new RandomWalker();
					walker.dir = RandomDirection();
					walker.pos = walkers[i].pos;
					walkers.Add(walker);
				}
			}

			//move Walkers
			for (int i = 0; i < walkers.Count; i++){
				RandomWalker walker = walkers[i];
				walker.pos += walker.dir;
				walkers[i] = walker;				
			}

			//avoid boarder of grid
			for (int i =0; i < walkers.Count; i++){
				RandomWalker walker = walkers[i];
				walker.pos.x = Mathf.Clamp(walker.pos.x, 1, levelWidth-2);
				walker.pos.y = Mathf.Clamp(walker.pos.y, 1, levelHeight-2);
				walkers[i] = walker;
			}

			//check to exit loop
			if ((float)NumberOfFloors() / (float)grid.Length > percentToFill){
				break;
			}
			iterations++;
		} while(iterations < iterationSteps);
	}
	
	void CreateWalls(){
		int bouncyQuota = Mathf.FloorToInt((float)NumberOfFloors() * 0.1f);
		int bouncyTally = 0;
		LevelTile tile2Place;
		bool isBouncy;
		
		for (int x = 0; x < levelWidth-1; x++) {
			for (int y = 0; y < levelHeight-1; y++) {
				if (grid[x,y] == LevelTile.floor) {
					if (isBouncy = (bouncyTally < bouncyQuota && Random.value < 0.1f)){ 
						tile2Place = LevelTile.bouncyWall;
					} else {
						tile2Place = LevelTile.wall;
					}
					
					//adjacent blocks                     bit redundant but hey
					if (grid[x,y+1] == LevelTile.empty || grid[x,y+1] == LevelTile.wall) {
						grid[x,y+1] = tile2Place;
						if (isBouncy) bouncyTally++;
					}
					if (grid[x,y-1] == LevelTile.empty || grid[x,y-1] == LevelTile.wall) {
						grid[x,y-1] = tile2Place;
						if (isBouncy) bouncyTally++;
					}
					if (grid[x+1,y] == LevelTile.empty || grid[x+1,y] == LevelTile.wall) {
						grid[x+1,y] = tile2Place;
						if (isBouncy) bouncyTally++;
					}
					if (grid[x-1,y] == LevelTile.empty || grid[x-1,y] == LevelTile.wall) {
						grid[x-1,y] = tile2Place;
						if (isBouncy) bouncyTally++;
					}
					//diagonal Blocks
                    if (grid[x - 1, y - 1] == LevelTile.empty 
							|| grid[x - 1, y - 1] == LevelTile.wall) {
                        grid[x - 1, y - 1] = tile2Place;
						if (isBouncy) bouncyTally++;
                    }
                    if (grid[x - 1, y + 1] == LevelTile.empty 
							|| grid[x - 1, y + 1] == LevelTile.wall) {
                        grid[x - 1, y + 1] = tile2Place;
						if (isBouncy) bouncyTally++;
                    }
                    if (grid[x + 1, y + 1] == LevelTile.empty 
							|| grid[x + 1, y + 1] == LevelTile.wall) {
                        grid[x + 1, y + 1] = tile2Place;
						if (isBouncy) bouncyTally++;
                    }
                    if (grid[x + 1, y - 1] == LevelTile.empty 
							|| grid[x + 1, y - 1] == LevelTile.wall) {
                        grid[x + 1, y - 1] = tile2Place;
						if (isBouncy) bouncyTally++;
                    }
                }
            }
		}
	}
	
	Vector2 RandomDirection(){
		int choice = Mathf.FloorToInt(Random.value * 3.99f);
		switch (choice){
			case 0:
				return Vector2.down;
			case 1:
				return Vector2.left;
			case 2:
				return Vector2.up;
			default:
				return Vector2.right;
		}
	}
	
	int NumberOfFloors() {
		int count = 0;
		foreach (LevelTile space in grid){
			if (space == LevelTile.floor){
				count++;
			}
		}
		return count;
	}
	
	/* nop dont cARE
	
	void CreateBottomWalls() {
		for (int x = 0; x < levelWidth; x++) {
			for (int y = 1; y < levelHeight; y++) {
				if (grid[x, y] == LevelTile.wall && grid[x, y - 1] == LevelTile.floor) {
                    grid[x, y] = LevelTile.bottomWall;
                }
            }
		}
	}
	
	
	*/
	
	void SpawnLevel(){
		for (int x = 0; x < levelWidth; x++) {
			for (int y = 0; y < levelHeight; y++) {
				switch(grid[x, y]) {
					case LevelTile.empty:
						break;
					case LevelTile.floor:
						floorTiles.SetTile(new Vector3Int(x, y, 0), floorTile); // set new floor
						break;
					case LevelTile.wall:
						wallTiles.SetTile(new Vector3Int(x, y, 0), wallTile);
						break;
					case LevelTile.bouncyWall:
						BouncyWallTiles.SetTile(new Vector3Int(x, y, 0), BouncyWallTile);
						break;
				}
			}
		}
	}    
	
	
	
		
	/*
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
	*/
}