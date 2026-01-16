using System.Collections;
using System.Collections.Generic;
using UnityEngine;

[ExecuteInEditMode]
public class TileManager : MonoBehaviour
{

    public bool start = true;
    public GameObject tilePrefab;
    public GameObject[,] tiles;
    public int width=17, height=30;
    // Start is called before the first frame update
    void Start()
    {
        tiles = new GameObject[width, height];
    }

    // Update is called once per frame
    void Update()
    {
        if (start)
        {
            start = false;
            
            for (int i = 0; i < width; i++)
            {
                for (int j = 0; j < height; j++)
                {
                    tiles[i, j] = Instantiate(tilePrefab, new Vector3(i, j, 0), Quaternion.identity);
                    tiles[i, j].GetComponent<Tile>().posX = i;
                    tiles[i, j].GetComponent<Tile>().posY = j;
                    tiles[i, j].GetComponent<Tile>().rePosition();
                }
            }
        }
    }
}