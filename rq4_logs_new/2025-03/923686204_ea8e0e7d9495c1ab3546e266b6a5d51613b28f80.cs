using System.Collections.Generic;
using UnityEngine;

public class FlowFieldNode
{
    public Vector3 worldPosition;
    public int gridX, gridY;
    public int baseCost;
    public int integrationCost;
    public Vector3 bestDirection;

    public FlowFieldNode(Vector3 worldPos, int gridX, int gridY, int baseCost)
    {
        this.worldPosition = worldPos;
        this.gridX = gridX;
        this.gridY = gridY;
        this.baseCost = baseCost;
        integrationCost = int.MaxValue;
        bestDirection = Vector3.zero;
    }
}

public class FlowField
{
    public FlowFieldNode[,] grid;
    public int gridSizeX, gridSizeY;
    public float cellSize;
    public Vector3 center;

    private Vector3 bottomLeft;

    public FlowField(int gridSizeX, int gridSizeY, float cellSize, Vector3 center)
    {
        this.gridSizeX = gridSizeX;
        this.gridSizeY = gridSizeY;
        this.cellSize = cellSize;
        this.center = center;

        bottomLeft = center - new Vector3((gridSizeX * cellSize) * 0.5f, 0, (gridSizeY * cellSize) * 0.5f);

        grid = new FlowFieldNode[gridSizeX, gridSizeY];
        for (int x = 0; x < gridSizeX; x++)
        {
            for (int y = 0; y < gridSizeY; y++)
            {

                Vector3 worldPos = bottomLeft + new Vector3(x * cellSize + cellSize * 0.5f, 0, y * cellSize + cellSize * 0.5f);
                grid[x, y] = new FlowFieldNode(worldPos, x, y, 1);
            }
        }
    }

    /// <summary>
    /// Converts a world position to grid indices using the computed bottomLeft
    /// </summary>
    public Vector2Int WorldPositionToIndex(Vector3 worldPosition)
    {
        // Use bottomLeft computed in the constructor.
        int x = Mathf.FloorToInt((worldPosition.x - bottomLeft.x) / cellSize);
        int y = Mathf.FloorToInt((worldPosition.z - bottomLeft.z) / cellSize);
        return new Vector2Int(x, y);
    }

    /// <summary>
    /// Computes the integration field using aDijkstra-style search starting at the target
    /// </summary>
    public void ComputeIntegrationField(Vector2Int targetIndex)
    {
        // Reset integration costs
        for (int x = 0; x < gridSizeX; x++)
        {
            for (int y = 0; y < gridSizeY; y++)
            {
                grid[x, y].integrationCost = int.MaxValue;
            }
        }

        List<FlowFieldNode> openList = new List<FlowFieldNode>();
        FlowFieldNode targetNode = grid[targetIndex.x, targetIndex.y];
        targetNode.integrationCost = 0;
        openList.Add(targetNode);

        int OBSTACLE_COST = FlowFieldManager.Instance.obstacleCost;

        while (openList.Count > 0)
        {
            openList.Sort((a, b) => a.integrationCost.CompareTo(b.integrationCost));
            FlowFieldNode current = openList[0];
            openList.RemoveAt(0);

            foreach (FlowFieldNode neighbor in GetNeighbors(current))
            {
                // Skip obstacles so that integration doesn't go through them
                if (neighbor.baseCost >= OBSTACLE_COST)
                    continue;

                int movementCost = (neighbor.gridX != current.gridX && neighbor.gridY != current.gridY) ? 14 : 10;
                int newCost = current.integrationCost + neighbor.baseCost * movementCost;
                if (newCost < neighbor.integrationCost)
                {
                    neighbor.integrationCost = newCost;
                    if (!openList.Contains(neighbor))
                        openList.Add(neighbor);
                }
            }
        }
    }

    /// <summary>
    /// Computes the best flow direction for every cell by examining its neighbors
    /// </summary>
    public void ComputeFlowField()
    {
        for (int x = 0; x < gridSizeX; x++)
        {
            for (int y = 0; y < gridSizeY; y++)
            {
                FlowFieldNode node = grid[x, y];
                List<FlowFieldNode> neighbors = GetNeighbors(node);
                int lowestCost = node.integrationCost;
                FlowFieldNode bestNode = null;
                foreach (FlowFieldNode neighbor in neighbors)
                {
                    if (neighbor.integrationCost < lowestCost)
                    {
                        lowestCost = neighbor.integrationCost;
                        bestNode = neighbor;
                    }
                }
                node.bestDirection = bestNode != null ? (bestNode.worldPosition - node.worldPosition).normalized : Vector3.zero;
            }
        }
    }

    /// <summary>
    /// Returns a list of neighboring nodes
    /// </summary>
    private List<FlowFieldNode> GetNeighbors(FlowFieldNode node)
    {
        List<FlowFieldNode> neighbors = new List<FlowFieldNode>();
        for (int dx = -1; dx <= 1; dx++)
        {
            for (int dy = -1; dy <= 1; dy++)
            {
                if (dx == 0 && dy == 0)
                    continue;
                int checkX = node.gridX + dx;
                int checkY = node.gridY + dy;
                if (checkX >= 0 && checkX < gridSizeX && checkY >= 0 && checkY < gridSizeY)
                {
                    neighbors.Add(grid[checkX, checkY]);
                }
            }
        }
        return neighbors;
    }

    /// <summary>
    /// Given a world position, returns the best flow direction from the corresponding cell
    /// </summary>
    public Vector3 GetDirection(Vector3 worldPosition)
    {
        Vector2Int index = WorldPositionToIndex(worldPosition);
        if (index.x >= 0 && index.x < gridSizeX && index.y >= 0 && index.y < gridSizeY)
            return grid[index.x, index.y].bestDirection;
        return Vector3.zero;
    }
}