using UnityEngine;
using System.Collections;

/// <summary>
/// エディターシーンのコントローラー
/// </summary>
public class EditorController : MonoBehaviour {

    /// <summary>
    /// 点オブジェクトのひな型
    /// </summary>
    public GameObject PointPrefab;

    /// <summary>
    /// 生成される全オブジェクトの親
    /// </summary>
    public Transform Objects;

    // 各レイヤー番号
    private int xyzLayer_;
    private int xywLayer_;
    private int wyzLayer_;

    /// <summary>
    /// オブジェクト生成時の処理
    /// </summary>
    void Awake()
    {
        xyzLayer_ = LayerMask.NameToLayer("SpaceXYZ");
        xywLayer_ = LayerMask.NameToLayer("SpaceXYW");
        wyzLayer_ = LayerMask.NameToLayer("SpaceWYZ");
    }

    /// <summary>
    /// クリック時の処理
    /// </summary>
    /// <param name="worldPosition">クリックされたワールド座標</param>
    /// <param name="layer">クリックされたレイヤー</param>
    public void OnClick(Vector3 worldPosition, int layer)
    {
        // 新しい点の生成
        GameObject newPoint = Instantiate<GameObject>(PointPrefab);
        newPoint.transform.parent = Objects;
        
        // 4次元位置を設定
        Vector4 pos = GetPosition4D(worldPosition, layer);
        newPoint.transform.position = pos;
        newPoint.GetComponent<PointController>().W = pos.w;
    }

    // 3次元座標とレイヤーから4次元座標を取得する。
    private Vector4 GetPosition4D(Vector3 pos, int layer)
    {
        if (layer == xyzLayer_)
        {
            return new Vector4(pos.x, pos.y, pos.z, 0.0f);
        }
        else if (layer == xywLayer_)
        {
            return new Vector4(pos.x, pos.y, 0.0f, pos.z);
        }
        else
        {
            return new Vector4(0.0f, pos.y, pos.z, pos.x);
        }
    }

    // Use this for initialization
    void Start () {
	
	}
	
	// Update is called once per frame
	void Update () {
	
	}
}