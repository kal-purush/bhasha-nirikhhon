using Microsoft.Xna.Framework.Graphics;

namespace MononokeEngine.Graphics.Tiles
{
    
    /// <summary>
    /// A Tile is just a plain image.
    /// It's much simpler than a sprite, since it doesn't deal with collisions,
    /// rects and other complex stuff.
    /// </summary>
    public class Tile
    {
        public int Width { get; }
        public int Height { get; }
        
        public Texture2D Texture { get; }
        
        
        public Tile(Texture2D texture)
        {
            Texture = texture;
            Width = texture.Width;
            Height = texture.Height;
        }
    }
}