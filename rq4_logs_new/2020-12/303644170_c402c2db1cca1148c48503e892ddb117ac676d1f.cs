using System;
using System.Collections;
//this Collections.Generic allows us to create and use lists
using System.Collections.Generic;
using UnityEngine;
//sets random as the UnityEngine, instead of the system random
using Random = UnityEngine.Random;

public class BoardManager : MonoBehaviour
{
    [Serializable]
    public class Count
    {
        public int minimum;
        public int maximum;

        //assignment constructor for Count
        public Count (int min, int max)
        {
            minimum = min;
            maximum = max;
        }
    }

    //declare our class variables

    //defines the size of our board, 8 x 8 game board
    public int columns = 8;
    public int rows = 8;
    //random range of how many walls and food we want to spawn
    public Count wallCount = new Count(5, 9);
    public Count foodCount = new Count(1, 5);
    //single game object for exiting
    public GameObject exit;
    //arrays of our other game objects
    public GameObject[] floorTiles;
    public GameObject[] wallTiles;
    public GameObject[] foodTiles;
    public GameObject[] enemyTiles;
    public GameObject[] outerWallTiles;

    //used to child all the game objects to keep elements organized
    //then they can all be collapsed in the hierarchy
    private Transform  boardHolder;
    //this List is used to track all the possible positions
    //on the game board, and keep track if an item has been
    //spawned on that position
    private List <Vector3> gridPositions = new List<Vector3>();

    //We are gonna clear list of gridpositions
    //then fill the list as a vector3
    //in other words, we are create a list of possible positions
    //for walls, enemies, food, etc.
    void IntialiseList()
    {
        gridPositions.Clear();
        
        for (int x = 1; x < columns -1; x++)
        {
            for (int y = 1; y < rows -1; y++)
            {   
                //creating a list, of possible locations for spawned items, 
                //e.g walls, food, etc
                gridPositions.Add(new Vector3(x, y, 0f));
            }
        }
    }
    //sets up outwall and the floor of gameBoard
    void BoardSetup ()
    {
        boardHolder = new GameObject ("Board").transform;

        for (int x = -1; x < columns + 1; x++)
        {
            for (int y = -1; y < rows + 1; y++)
            {   
                //creating floor tiles at random
                GameObject toInstantiate = floorTiles[Random.Range (0, floorTiles.Length)];
                //now check these conditions, if so create from outer walls array
                if (x == -1 || x == columns || y == -1 || y == rows)
                    //creating wall tiles at random
                    toInstantiate = outerWallTiles[Random.Range (0, outerWallTiles.Length)];

                GameObject instance = Instantiate(toInstantiate, new Vector3 (x, y, 0f), Quaternion.identity);

                instance.transform.SetParent(boardHolder);
            }
        }
    }

    //this function returns a vector3 - sets the random objections on the gameBoard
    Vector3 RandomPosition()
    {
        int randomIndex = Random.Range(0, gridPositions.Count);
        Vector3 randomPosition = gridPositions[randomIndex];
        gridPositions.RemoveAt(randomIndex);
        return randomPosition;
    }

    void LayoutObjectAtRandom(GameObject[] tileArray, int minimum, int maximum)
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
        BoardSetup();
        IntialiseList();
        LayoutObjectAtRandom(wallTiles, wallCount.minimum, wallCount.maximum);
        LayoutObjectAtRandom(foodTiles, foodCount.minimum, foodCount.maximum);
        int enemyCount = (int)Mathf.Log(level, 2f);
        LayoutObjectAtRandom(enemyTiles, enemyCount, enemyCount);
        Instantiate(exit, new Vector3(columns - 1, rows -1, 0f), Quaternion.identity);
    }
}