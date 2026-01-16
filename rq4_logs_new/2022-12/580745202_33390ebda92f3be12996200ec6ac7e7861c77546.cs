using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mishbetzet
{
    /// <summary>
    /// The core interface for the engine
    /// </summary>
    public class Core : Engine , IRender
    {
        public Tilemap Tilemap { get; private set; }

        public override bool IsRunning => _isRunning;

        public event Action? onEngineStart;
        public event Action? onEngineStop;

        bool _isRunning = false;

        public Core(Tilemap tilemap)
        {
            Tilemap = tilemap;
        }

        public void AddTile(int x, int y)
        {
            Tilemap.AddTile(x, y);
        }

        public GameObject CreateGameObject(Tile tile)
        {
            return GameObject.Create(tile);
        }

        public override void Play()
        {
            onEngineStart?.Invoke();
        }

        public override void Stop()
        {
            onEngineStop?.Invoke();
        }
    }
}