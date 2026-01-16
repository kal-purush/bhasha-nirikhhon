using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/// <summary>
/// マップを生成する
/// </summary>
public class MazeMapping : MonoBehaviour
{
    /// <summary>マップの名前</summary>
    [SerializeField] string m_mapName = "map";
    /// <summary>マップのサイズ（奇数）</summary>
    [Header("マップサイズ※５以上の奇数を入力")]
    [SerializeField] int m_mapSize;
    /// <summary>作成する階層</summary>
    [SerializeField] int m_mapFloor;
    /// <summary>キューブプレハブ</summary>
    [SerializeField] GameObject m_cubePrefab = null;
    /// <summary>床キューブプレハブ</summary>
    [SerializeField] GameObject m_floorCubePrefab = null;
    /// <summary>スタートキューブプレハブ</summary>
    [SerializeField] GameObject m_startCubePrefab = null;
    /// <summary>中間地点キューブプレハブ</summary>
    [SerializeField] GameObject m_middleCubePrefab = null;
    /// <summary>ゴールキューブプレハブ</summary>
    [SerializeField] GameObject m_goalCubePrefab = null;
    /// <summary>プレイヤープレハブ</summary>
    [SerializeField] GameObject m_playerPrefab = null;
    /// <summary>デバッグonoff</summary>
    [SerializeField] bool m_debug = true;
    /// <summary>穴掘りメソッド</summary>
    DrillingMethod m_drillingMethod = null;
    List<GameObject> m_maps = new List<GameObject>();

    /// <summary>マップを消します</summary>
    public void DeletMap()
    {
        m_maps.ForEach(map => Destroy(map));
    }

    /// <summary>迷路マップ</summary>
    public void CreateMap()
    {
        m_drillingMethod = new DrillingMethod(m_mapName, m_mapSize, m_cubePrefab, m_floorCubePrefab, m_startCubePrefab,
                    m_middleCubePrefab, m_goalCubePrefab, m_playerPrefab);
        m_drillingMethod.FlatMapping();
        //マップデータを入れる
        m_maps.Add(m_drillingMethod.GetMapObject);
    }

    /// <summary>階層マップ生成</summary>
    public void CreateFloorMap()
    {

    }
}