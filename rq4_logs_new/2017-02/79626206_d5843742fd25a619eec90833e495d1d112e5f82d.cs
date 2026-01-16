using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class TileMap : MonoBehaviour {

    public TileType[] tileTypes;
    public enum NodeType { Blank = 0,StaticL, StaticLine,StaticCross, StaticT,MoveLine,MobeL,MoveT, BSource,GSource,RSource,End };
    int[,] tiles;
    GraphNode[,] graph;
    public int mapSizeX = 10;
    public int mapSizeY = 10;
    void Start()
    {
        GenerateMapData();
        GenerateGraph();
        //Spawn visual prefabs
        GenerateMapVisual();
    }

    void GenerateMapData()
    {
        //Allocate Map tiles
        tiles = new int[mapSizeX, mapSizeY];

        //Initialize map tiles
        for (int x = 0; x < mapSizeX; x++)
        {
            for (int y = 0; y < mapSizeY; y++)
            {
                tiles[x, y] = 0;
            }
        }
    }
    void GenerateMapVisual()
    {
        for (int x = 0; x < mapSizeX; x++)
        {
            for (int y = 0; y < mapSizeY; y++)
            {
                TileType tt = tileTypes[tiles[x, y]];
                GameObject tile = (GameObject)Instantiate(tt.tileVisualPrefab, new Vector2(x*3, y*3), Quaternion.identity);
                Node node = tile.GetComponent<Node>();
                node.tileX = x;
                node.tileY = y;
            }
        }

    }

    class GraphNode
    {
        public List<GraphNode> neighbours;
        public GraphNode()
        {
            neighbours = new List<GraphNode>();
        }
    }
    void GenerateGraph()
    {
        graph = new GraphNode[mapSizeX, mapSizeY];
        for(int x = 0; x< mapSizeX; x++)
        {
            for(int y = 0; y < mapSizeY; y++)
            {
                if(x> 0)
                {
                    graph[x, y].neighbours.Add(graph[x - 1, y]);
                }
                if(x< mapSizeX-1)
                {
                    graph[x, y].neighbours.Add(graph[x + 1, y]);
                }
                if(y> 0)
                {
                    graph[x, y].neighbours.Add(graph[x, y-1]);
                }
                if(y< mapSizeY-1)
                {
                    graph[x, y].neighbours.Add(graph[x, y+1]);
                }
                
            }
        }

    }
}