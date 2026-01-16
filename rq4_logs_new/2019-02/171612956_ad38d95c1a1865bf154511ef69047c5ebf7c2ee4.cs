using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class LevelPieceCreator : MonoBehaviour
{
    [Header("Board Stats")]
    public int columns = 3;
    public int rows = 3;
    public List<List<int>> level = new List<List<int>>();

    public List<GameObject> outerTiles;
 
    // Start is called before the first frame update
    void Start()
    {
        InitializeLevel();
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    void InitializeLevel()
    {
        for(int r = 0; r < rows; r++)
        {
            List<int> temp = new List<int>();
            temp.Clear();
            for(int col = 0; col < columns; col++)
            {
                temp.Add(0);
            }
            level.Add(temp);
        }

        for (int r = 0; r < level.Count; r++)
        {
            
            for (int col = 0; col < level[r].Count; col++)
            {
                GameObject toInstantiate = outerTiles[Random.Range(0, outerTiles.Count)];
                GameObject createdTile = Instantiate(toInstantiate, new Vector3(r, col, 0), Quaternion.identity);

            }
        }
    }
}