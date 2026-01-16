using UnityEngine;
using UnityEngine.Tilemaps;

public class AutoTileAssigner : MonoBehaviour
{
    public Tilemap tilemap;
    public TileBase[] tiles;
    public float tileCoverage = 1f;
    public int tileSizeX = 1;
    public int tileSizeY = 1;
    public int minusTileSizeX = 1;
    public int minusTileSizeY = 1;
    

    void Start()
    {
        GenerateTiles();
    }

    void GenerateTiles()
    {
        // Tilemap'in boyutlarını al
        BoundsInt bounds = tilemap.cellBounds;

        // Tüm tile'ları temizle
        tilemap.ClearAllTiles();

        int startX = bounds.min.x;
        int startY = bounds.min.y;

        // Tüm tile'ları dolaş
        for (int x = startX; x < bounds.max.x; x += tileSizeX)
        {
            for (int y = startY; y < bounds.max.y; y += tileSizeY)
            {
                // Belirli bir oranda tile ata
                if (Random.value <= tileCoverage)
                {
                    TileBase randomTile = tiles[Random.Range(0, tiles.Length)];

                    // Tile'ları boyutuyla birlikte ata
                    for (int i = x; i < x + tileSizeX; i++)
                    {
                        for (int j = y; j < y + tileSizeY; j++)
                        {
                            tilemap.SetTile(new Vector3Int(i, j, 0), randomTile);
                        }
                    }
                }
            }
        }
        for (int x = startX; x < bounds.max.x; x += minusTileSizeX)
        {
            for (int y = startY; y < bounds.max.y; y += minusTileSizeY)
            {
                // Belirli bir oranda tile ata
                if (Random.value <= tileCoverage)
                {
                    TileBase randomTile = tiles[Random.Range(0, tiles.Length)];

                    // Tile'ları boyutuyla birlikte ata
                    for (int i = x; i < x + minusTileSizeX; i++)
                    {
                        for (int j = y; j < y + minusTileSizeY; j++)
                        {
                            tilemap.SetTile(new Vector3Int(i, -j, 0), randomTile);
                        }
                    }
                }
            }
            for (int xa = startX; xa < bounds.max.x; xa += minusTileSizeX)
            {
                for (int y = startY; y < bounds.max.y; y += minusTileSizeY)
                {
                    // Belirli bir oranda tile ata
                    if (Random.value <= tileCoverage)
                    {
                        TileBase randomTile = tiles[Random.Range(0, tiles.Length)];

                        // Tile'ları boyutuyla birlikte ata
                        for (int i = x; i < x + minusTileSizeX; i++)
                        {
                            for (int j = y; j < y + minusTileSizeY; j++)
                            {
                                tilemap.SetTile(new Vector3Int(-i, j, 0), randomTile);
                            }
                        }
                    }
                }
            }
            for (int xa = startX; xa < bounds.max.x; xa += minusTileSizeX)
            {
                for (int y = startY; y < bounds.max.y; y += minusTileSizeY)
                {
                    // Belirli bir oranda tile ata
                    if (Random.value <= tileCoverage)
                    {
                        TileBase randomTile = tiles[Random.Range(0, tiles.Length)];

                        // Tile'ları boyutuyla birlikte ata
                        for (int i = x; i < x + minusTileSizeX; i++)
                        {
                            for (int j = y; j < y + minusTileSizeY; j++)
                            {
                                tilemap.SetTile(new Vector3Int(-i, -j, 0), randomTile);
                            }
                        }
                    }
                }
            }
        }
    }
}