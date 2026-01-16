using System;
using System.Collections.Generic;
using DefaultEcs;
using DefaultEcs.System;
using GigglyLib.Components;
using Microsoft.Xna.Framework.Graphics;

namespace GigglyLib.Systems
{
    public class EnemySpawnSys : ISystem<float>
    {
        World _world;
        Entity _player;
        Texture2D _enemyTexture;

        public EnemySpawnSys(World world, Entity player, Texture2D enemyTexture)
        {
            _world = world;
            _player = player;
            _enemyTexture = enemyTexture;
        }

        public bool IsEnabled { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        public void Dispose() { }

        public void Update(float state)
        {
            if (new Random().Next(1, 101) <= 5)
            {
                var pPos = _player.Get<CGridPosition>();
                var spawnPositions = new List<(int x, int y)>
                {
                    (0, 0),
                    (2, 3),
                    (-2, 3),
                    (0, 7),
                    (-3, 6),
                    (3, 6)
                };

                for (int i = 0; i < spawnPositions.Count; i++)
                {
                    spawnPositions[i] = (spawnPositions[i].x, spawnPositions[i].y - 20);
                    for (int j = 0; j < (int)pPos.Facing; j++)
                        spawnPositions[i] = (-spawnPositions[i].y, spawnPositions[i].x);
                    var (x, y) = spawnPositions[i];
                    x += pPos.X;
                    y += pPos.Y;

                    var enemy = _world.CreateEntity();
                    enemy.Set(new CEnemy());
                    enemy.Set(new CGridPosition { X = x, Y = y, Facing =
                        pPos.Facing == Direction.NORTH ? Direction.SOUTH :
                        pPos.Facing == Direction.SOUTH ? Direction.NORTH :
                        pPos.Facing == Direction.EAST ? Direction.WEST :
                        Direction.EAST
                        });
                    enemy.Set(new CMovable());
                    enemy.Set(new CHealth { Max = 15 });
                    enemy.Set(new CWeapon
                    {
                        Damage = 5,
                        RangeFront = 5,
                        RangeLeft = 1,
                        RangeRight = 1,
                        RangeBack = 0,
                        AttackPattern = new List<string>
                        {
                            "0"
                        }
                    });
                    enemy.Set(new CSprite { Texture = _enemyTexture, Depth = 0.25f, X = x * Config.TileSize, Y = y * Config.TileSize, });
                }
            }
        }
    }
}