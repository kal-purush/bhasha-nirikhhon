using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Game : MonoBehaviour
{
    public GameObject DangerTile;
    public GameObject SafeTile;

    int[,] maze;

    int[] RandomPosition()
    {
        return Random.Range(0,5);
    }

    void GenerateMaze()
    {
        var new_maze = new int[5,5];

    }

    void Awake()
    {
        maze = new int[,] {{ 1, 1, 0, 0, 0 }, 
                           { 0, 1, 0, 0, 0 }, 
                           { 0, 1, 1, 0, 0 }, 
                           { 0, 0, 1, 0, 0 },
                           { 0, 0, 1, 0, 0 }};
    }

    void Draw()
    {
        float fix = 1.1f * 2;

        for(int y = 0; y < 5; y = y + 1)
        {
            for(int x = 0; x < 5; x = x + 1)
            {
                GameObject plot;

                switch(maze[y,x])
                {
                    case 1:
                        plot = SafeTile;
                        break;
                    default:
                        plot = DangerTile;
                        break;
                }

                Instantiate(plot, new Vector3((1.1f * x) - fix, (-1.1f * y) + fix, 1), Quaternion.identity);
            }
        }
    }

    // Start is called before the first frame update
    void Start()
    {
        Draw();
        
    }

    // Update is called once per frame
    void Update()
    {
        
    }
}