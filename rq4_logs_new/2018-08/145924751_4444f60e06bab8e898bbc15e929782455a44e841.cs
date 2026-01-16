using System.Collections.Generic;
using UnityEngine;

public class BoardController : MonoBehaviour {
    public GameObject emptyTile;
    public GameObject placedTile;
    private Board board;
    private Transform boardObject;
    private List<Vector3> gridPositions = new List<Vector3>();

    public void SetupBoard() {
        //Creates the empty tiles
        BoardSetup();

        //Reset our list of gridpositions.
        InitialiseTileList();
    }

    private void BoardSetup() {
        //Instantiate Board and set boardHolder to its transform.
        boardObject = new GameObject("Board").transform;

        //Loop along x axis, starting from -1 (to fill corner) with floor or outerwall edge tiles.
        for (int x = -1; x < board.Columns + 1; x++) {
            //Loop along y axis, starting from -1 to place floor or outerwall tiles.
            for (int y = -1; y < board.Rows + 1; y++) {
                //Choose a random tile from our array of floor tile prefabs and prepare to instantiate it.
                GameObject toInstantiate = emptyTile;

                //Instantiate the GameObject instance using the prefab chosen for toInstantiate at the Vector3 corresponding to current grid position in loop, cast it to GameObject.
                GameObject instance =
                    Instantiate(toInstantiate, new Vector3(x, y, 0f), Quaternion.identity) as GameObject;

                //Set the parent of our newly instantiated object instance to boardObject, this is just organizational to avoid cluttering hierarchy.
                instance.transform.SetParent(boardObject);
            }
        }
    }

    private void InitialiseTileList() {
        //Clear our list gridPositions.
        gridPositions.Clear();

        for (int x = 1; x < board.Columns - 1; x++) {
            for (int y = 1; y < board.Columns - 1; y++) {
                gridPositions.Add(new Vector3(x, y, 0f));
            }
        }
    }
}