using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class Description : MonoBehaviour {

    [SerializeField]
    private Text description;

    void Start() {
        description.text = "赤エリア：オブジェクトの大きさが変わる。\n\n" +
        "青エリア：オブジェクトの色と振れ幅が変わる。\n\n" +
        "※両エリアに入ると両方の変化が適用される。";
        description.fontSize = 32;
    }
}