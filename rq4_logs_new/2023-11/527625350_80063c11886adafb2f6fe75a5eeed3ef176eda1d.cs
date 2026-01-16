using Microsoft.Xna.Framework;
using MonoGame.Extended.Sprites;
using MonoGame.Extended.TextureAtlases;
using MonoGame.Extended.Tiled;
using System;
using System.Collections.Generic;

namespace Platformer.Factories
{
    public class TiledSpriteFactory
    {
        private TiledMap _tiledMap;
        private Dictionary<string, TextureAtlas> _atlases;

        public TiledSpriteFactory(TiledMap tiledMap)
        {
            _tiledMap = tiledMap;

            _atlases = new Dictionary<string, TextureAtlas>(_tiledMap.Tilesets.Count);
            foreach (var tileset in tiledMap.Tilesets)
            {
                Dictionary<string, Rectangle> regions = new(tileset.TileCount);
                for (int i = 0; i < tileset.TileCount; i++)
                {
                    regions.Add(i.ToString(), tileset.GetTileRegion(i));
                }
                _atlases.Add(tileset.Name, new(tileset.Name, tileset.Texture, regions));
            }
        }

        internal AnimatedSprite CreateAnimatedSprite(TiledMapTileObject tileObject)
        {
            var animatedTile = tileObject.Tile as TiledMapTilesetAnimatedTile;
            SpriteSheet sheet = new();
            sheet.TextureAtlas = _atlases[tileObject.Tileset.Name];
            SpriteSheetAnimationCycle cycle = new()
            {
                IsLooping = true
            };
            foreach (var frame in animatedTile.AnimationFrames)
            {
                SpriteSheetAnimationFrame ssFrame = new(frame.LocalTileIdentifier, (float)frame.Duration.TotalSeconds);
                cycle.Frames.Add(ssFrame);
            }
            sheet.Cycles.Add(tileObject.Name, cycle);
            return new AnimatedSprite(sheet, tileObject.Name);
        }

        internal Sprite CreateSprite(TiledMapTileObject tileObject)
        {
            if (tileObject.Tile == null) return null;

            int col = tileObject.Tile.LocalTileIdentifier % tileObject.Tileset.Columns;
            int row = tileObject.Tile.LocalTileIdentifier / tileObject.Tileset.Columns;
            var region = tileObject.Tileset.GetRegion(col, row);
            return new Sprite(region);
        }
    }
}