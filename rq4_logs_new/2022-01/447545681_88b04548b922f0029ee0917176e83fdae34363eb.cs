using System;
using System.Collections.Generic;
using System.Text;
using MonoGame.Extended.Tiled;

namespace lost_clothes_code
{
    public class Global
    {
        
        public static bool IsCollision(TiledMapTileLayer mapLayer, ushort x, ushort y)
        {
            // détermine si la tuile en (x,y) est un obstacle (mur ou objet)
            if (mapLayer.GetTile(x, y).GlobalIdentifier > 0 && mapLayer.GetTile(x, y).GlobalIdentifier < 43)
                return true;

            return false;
        }
    }
}