using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using Microsoft.Xna.Framework;

class SpecialLevel : Level 
{
    TileGrid tileGrid;

    public SpecialLevel(int roomNumber, string name) : base(roomNumber)
    {
        tileGrid = LoadLevel(name);
        gameObjects.Add(tileGrid);
        Player player = new Player(Vector3.Zero);
        gameObjects.Add(player);
    }

    public TileGrid LoadLevel(string name)
    {
        List<string> text = new List<string>();
        StreamReader streamReader = new StreamReader(name);
        string line = streamReader.ReadLine();
        int width = line.Length;
        while (line != null)
        {
            text.Add(line);
            line = streamReader.ReadLine();
        }
        TileGrid tilegrid = new TileGrid(text.Count, width, "grid");
        for (int x = 0; x < width; ++x)
        {
            for (int y = 0; y < text.Count - 1; ++y)
            {
                Tile tile = LoadTile(text[y][x], x, y);
                tileGrid.Add(tile, x, y);
            }
        }
        return tileGrid;
    }

    public Tile LoadTile(char chr, int x, int y)
    {
        if (chr == 'W')
            return new WallTile();
        else if (chr == 'P')
            return new PathTile();
        else if (chr == 'N')
            return new EntryTile();
        else if (chr == 'X')
            return new ExitTile();
        else
            return null;
    }
}