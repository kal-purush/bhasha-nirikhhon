using System.Collections.Generic;
using UnityEngine;

public class DebugPathFinder : MonoBehaviour
{
    public static DebugPathFinder Instance { get; private set; }

    private void Awake()
    {
        Instance = this;
    }

    private int minX, maxX, minY, maxY; 
    private List<Vector2Int> walkableTiles;

    public void SetRoom(int xSize, int ySize, int xPos, int yPos, List<Vector2Int> walkableTiles)
    {
        minX = xPos;
        maxX = xPos + xSize;
        minY = yPos;
        maxY = yPos + ySize;
        this.walkableTiles = walkableTiles;
    }
    /// <summary>
    /// Returns the positions of the neighbours that are walkable tiles of a given tile.
    /// </summary>
    /// <param name="x">x-value of the tile to check</param>
    /// <param name="y">y-value of the tile to check</param>
    /// <returns>The positions of the neighbours that are walkable tiles of a given tile.</returns>
    private Vector2Int[] GetNeighbours(int x, int y)
    {
        List<Vector2Int> neighbours = new List<Vector2Int>();

        for (int x2 = x - 1; x2 < x + 2; x2++)
        {
            for (int y2 = y - 1; y2 < y + 2; y2++)
            {
                if (x2 >= minX && x2 < maxX && y2 >= minY && y2 < maxY && walkableTiles.Contains(new Vector2Int(x2, y2)))
                    neighbours.Add(new Vector2Int(x2, y2));
            }
        }
        return neighbours.ToArray();
    }

    /// <summary>
    /// Gets the pathfinding cost of a given tile
    /// </summary>
    /// <param name="tile"></param>
    /// <returns>Returns the pathfinding cost of a given tile</returns>
    private float GetCost(/*Vector2Int tile*/)
    {
        return 5f;
        // calculate costs here
        //return Map[tile.x, tile.y].specialType == SpecialTypes.None || Map[tile.x, tile.y].specialType == SpecialTypes.FloorToWater ? 5f : 20f;
    }

    /// <summary>
    /// Tries to find a new path through the cave using A*
    /// </summary>
    /// <param name="start">Where the pathfinding should start</param>
    /// <param name="destination">The destination of the path</param>
    /// <param name="path">All tile positions of the path found</param>
    /// <returns>Wheter it found a path or not</returns>
    public bool TryFindPath(Vector2Int start, Vector2Int destination, out List<Vector2> path)
    {
        Dictionary<Vector2Int, Vector2Int> cameFrom = new Dictionary<Vector2Int, Vector2Int>();
        Dictionary<Vector2Int, float> costSoFar = new Dictionary<Vector2Int, float>();

        Vector2Int current;

        PriorityQueue<Vector2Int> border = new PriorityQueue<Vector2Int>();
        border.Enqueue(start, 0f);

        cameFrom.Add(start, start);
        costSoFar.Add(start, 0f);

        while (border.Count > 0)
        {
            current = border.Dequeue();

            if (current.Equals(destination)) break;

            foreach (Vector2Int neighbour in GetNeighbours(current.x, current.y))
            {
                float newCost = costSoFar[current] + GetCost(/*neighbour*/);

                if (!costSoFar.ContainsKey(neighbour) || newCost < costSoFar[neighbour])
                {
                    if (costSoFar.ContainsKey(neighbour))
                    {
                        costSoFar.Remove(neighbour);
                        cameFrom.Remove(neighbour);
                    }

                    costSoFar.Add(neighbour, newCost);
                    cameFrom.Add(neighbour, current);
                    float priority = newCost + Mathf.Abs(neighbour.x - destination.x) + Mathf.Abs(neighbour.y - destination.y);
                    border.Enqueue(neighbour, priority);
                }
            }
        }

        path = new List<Vector2>();
        current = destination;

        while (!current.Equals(start))
        {
            if (!cameFrom.ContainsKey(current))
            {
                //Debug.Log("cameFrom does not contain current"); // no path found
                return false;
            }

            path.Add(new Vector2(current.x + 0.5f, current.y + 0.5f));
            current = cameFrom[current];
        }

        path.Reverse();

        if (path.Count > 1)
            path.RemoveAt(0);

        return true;
    }

    /// <summary>
    /// PriorityQueue for use within the A* path finding
    /// https://gist.github.com/keithcollins/307c3335308fea62db2731265ab44c06
    /// </summary>
    /// <typeparam name="T"></typeparam>
    private class PriorityQueue<T>
    {
        // From Red Blob: I'm using an unsorted array for this example, but ideally this
        // would be a binary heap. Find a binary heap class:
        // * https://bitbucket.org/BlueRaja/high-speed-priority-queue-for-c/wiki/Home
        // * http://visualstudiomagazine.com/articles/2012/11/01/priority-queues-with-c.aspx
        // * http://xfleury.github.io/graphsearch.html
        // * http://stackoverflow.com/questions/102398/priority-queue-in-net

        private readonly List<KeyValuePair<T, float>> elements = new List<KeyValuePair<T, float>>();

        public int Count
        {
            get { return elements.Count; }
        }

        public void Enqueue(T item, float priority)
        {
            elements.Add(new KeyValuePair<T, float>(item, priority));
        }

        // Returns the Location that has the lowest priority
        public T Dequeue()
        {
            int bestIndex = 0;

            for (int i = 0; i < elements.Count; i++)
            {
                if (elements[i].Value < elements[bestIndex].Value)
                {
                    bestIndex = i;
                }
            }

            T bestItem = elements[bestIndex].Key;
            elements.RemoveAt(bestIndex);
            return bestItem;
        }
    }
}