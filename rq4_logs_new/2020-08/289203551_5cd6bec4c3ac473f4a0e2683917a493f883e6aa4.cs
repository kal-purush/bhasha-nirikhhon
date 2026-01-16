using UnityEngine;

public class CanvasGrid : MonoBehaviour
{
    public RectTransform worldCanvasRect;
    public Transform tileParent;
    public CanvasTile tilePrefab;

    CanvasTile[,] canvasTiles;
    public void Setup(Grid<ObjectTile> grid)
    {
        if (canvasTiles != null)
        {
            foreach (var tile in canvasTiles)
            {
                Destroy(tile.gameObject);
            }
        }

        worldCanvasRect.sizeDelta = new Vector2(grid.Width, grid.Height);
        worldCanvasRect.position = new Vector3((float)grid.Width / 2 * grid.cellSize, (float)grid.Height / 2 * grid.cellSize);

        canvasTiles = new CanvasTile[grid.Width, grid.Height];

        for (int y = 0; y < canvasTiles.GetLength(1); y++)
        {
            for (int x = 0; x < canvasTiles.GetLength(0); x++)
            {
                CanvasTile tile = Instantiate(tilePrefab, tileParent);
                canvasTiles[x, y] = tile;
                ObjectTile objectTile = grid.GetGridObject(x, y);
                objectTile.CanvasTileObject = tile;
            }
        }
    }

    public void SetGridOutlines(bool status)
    {
        foreach (var tile in canvasTiles)
        {
            tile.SetGridOutline(status);
        }
    }
}