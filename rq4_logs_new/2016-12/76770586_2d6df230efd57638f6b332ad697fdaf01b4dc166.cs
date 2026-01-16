using System.Collections.Generic;
using UnityEngine;
using ZG.Network;

public class Test : MonoBehaviour
{
	void Start ()
    {
        List<Mahjong.RuleNode> ruleNodes = new List<Mahjong.RuleNode>();
        Mahjong.Rule rule = new Mahjong.Rule();
        Debug.Log(rule.Win(new int[]
        {
            new Mahjong.Tile(Mahjong.TileType.Dots, 0) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 0) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 1) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 1) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 1) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 2) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 2) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 2) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 2) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 3) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 3) << 2,
            new Mahjong.Tile(Mahjong.TileType.Dots, 4) << 2,
            new Mahjong.Tile(Mahjong.TileType.Bomboo, 0) << 2,
        }));
	}
	
}