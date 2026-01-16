using System.Collections.Generic;
using UnityEngine;
using System;
using System.Linq;
using UnityEngine.Tilemaps;
using System.Threading;
using UnityEngine.Events;
using static RuinsRaiders.AStarSharp.Astar;

namespace RuinsRaiders
{
    [RequireComponent(typeof(Tilemap))]
    public class AStar : MonoBehaviour
    {
        public UnityEvent onMapUpdated = new();

        private Tilemap _tilemap;
        private readonly HashSet<Vector2Int> _dynamicTiles = new();
        private readonly Dictionary<Vector2Int, AStarSharp.Node> _cache = new();

        private bool _mapUpdated = false;

        public void AddDynamicBlockTile(Vector2Int tileId)
        {
            _dynamicTiles.Add(tileId);
            _mapUpdated = true;
        }

        public void RemoveDynamicBlockTile(Vector2Int tileId)
        {
            _dynamicTiles.Remove(tileId);
            _mapUpdated = true;
        }

        private void Awake()
        {
            _tilemap = GetComponent<Tilemap>();
        }

        private void FixedUpdate()
        {
            if (_mapUpdated)
            {
                onMapUpdated.Invoke();
                _mapUpdated = false;
            }
        }

        public AStarSharp.Node GetNode(Vector2Int Id)
        {
            var cachedNode = _cache.GetValueOrDefault(Id);
            if (cachedNode == null)
            {
                cachedNode = new AStarSharp.Node
                {
                    Id = Id
                };
                var collider = _tilemap.GetColliderType(new Vector3Int(Id.x, Id.y, 0));
                var gameobject = _tilemap.GetInstantiatedObject(new Vector3Int(Id.x, Id.y, 0));
                if (gameobject != null)
                {
                    cachedNode.Spike = gameobject.GetComponentInChildren<SpikeTile>() != null;
                    cachedNode.Ladder = gameobject.GetComponentInChildren<LadderTile>() != null;
                    cachedNode.Platform = gameobject.GetComponentInChildren<PlatformTile>() != null;
                }
                cachedNode.Block = collider != Tile.ColliderType.None;
                cachedNode.Weight = 1;
                cachedNode.Cost = 1;

                _cache.Add(Id, cachedNode);
            }

            var node = cachedNode.Clone();

            if (node.Block)
                return node;

            node.Destroyable = _dynamicTiles.Contains(Id);
            node.Block = node.Destroyable;

            return node;
        }

        public Vector2 GetPositionFromId(Vector2Int id)
        {
            var pos = _tilemap.CellToWorld(new Vector3Int(id.x, id.y, 0));
            return new Vector2(pos.x + 0.5f, pos.y + 0.5f);
        }

        public Vector2Int GetTileId(Vector2 position)
        {
            if (_tilemap == null)
                return new Vector2Int(0, 0);

            var pos = _tilemap.WorldToCell(position);
            return new Vector2Int(pos.x, pos.y);
        }


        public Vector2Int GetTileIdLocal(Vector2 position)
        {
            var pos = _tilemap.LocalToCell(position);
            return new Vector2Int(pos.x, pos.y);
        }

        public List<AStarSharp.Node> GetNearbyNodes(AStarSharp.Node node, WalkData data)
        {
            var list = new List<AStarSharp.Node>();

            if (node.Spike)
                return list;

            if (data.flying)
                return FlyingNodesProvider.GetNodes(this, data, node);
            else
                return WalkingNodesProvider.GetNodes(this, data, node);

        }

        //public Stack<AStarSharp.Node> GetPath(Vector2Int start, Vector2Int end, WalkData data, int maxNodes = 600, CancellationToken ct = default)
        //{
        //    var astar = new AStarSharp.Astar();
        //    astar.GetAdjacentNodes = (it) => { return GetNearbyNodes(it, data); };
        //    astar.GetNode = (it) => { return GetNode(it); };
        //    return astar.FindPath(start, end, maxNodes, ct);
        //}

        public IEnumerator<FindPathStatus> GetPath(Vector2Int start, Vector2Int end, WalkData data, int maxNodesPerBatch = 10, int maxNodes = 600, CancellationToken ct = default)
        {
            var astar = new AStarSharp.Astar
            {
                GetAdjacentNodes = (it) => { return GetNearbyNodes(it, data); },
                GetNode = (it) => { return GetNode(it); }
            };
            return astar.FindPath(start, end, maxNodesPerBatch, maxNodes, ct);
        }
    }




}