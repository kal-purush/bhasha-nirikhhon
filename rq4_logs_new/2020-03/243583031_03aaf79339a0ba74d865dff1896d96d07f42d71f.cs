using System;
using System.Collections.Generic;
using DefaultEcs;
using GigglyLib.Components;

namespace GigglyLib.ProcGen
{
    public class MapGenerator
    {
        World _world;
        public MapGenerator(World world) { _world = world; }

        public void Generate()
        {
            SpawnEnemy(5, 5);
            SpawnEnemy(10, 7);
            SpawnEnemy(7, 2);
        }

        public Entity SpawnEnemy(int gX, int gY)
        {
            var e = _world.CreateEntity();
            e.Set<CEnemy>();
            e.Set(new CGridPosition { X = gX, Y = gY, Facing = Direction.WEST });
            e.Set<CMovable>();
            e.Set(new CHealth { Max = 15 });
            e.Set(Config.Weapons[new List<string>(Config.Weapons.Keys)[new Random().Next(0, Config.Weapons.Count)]]);
            e.Set(new CSprite { Texture = Config.Textures["enemy"], Depth = 0.25f, X = gX * Config.TileSize, Y = gY * Config.TileSize, });
            return e;
        }
    }
}