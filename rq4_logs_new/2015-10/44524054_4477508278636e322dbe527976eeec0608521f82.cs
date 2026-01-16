using UnityEngine;
using System.Collections;

public class CreateShot : MonoBehaviour
{

    public bool IsAttackAnime { get; set; }
    public bool IsAttackIdol { get; set; }
    public bool IsAttackGame { get; set; }
    public bool IsAttackVocalRob { get; set; }
    public bool IsAttackRobot { get; set; }

    [SerializeField]
    GameObject _bolloonPrefab = null;

    [SerializeField]
    GameObject _animeIconPrefab = null;
    [SerializeField]
    GameObject _idolIconPrefab = null;
    [SerializeField]
    GameObject _gameIconPrefab = null;
    [SerializeField]
    GameObject _vocalRobIconPrefab = null;
    [SerializeField]
    GameObject _robotIconPrefab = null;

    const string _shotManagerName = "ShotManager";

    //--------------------------------------

    void Start()
    {
        //TODO:それぞれのプレハブを複製
        //（※吹き出しショットとアイコンの生成
        //　　アイコンはショットの裏に隠れるように生成して
        //　　ショットが徐々に縮小し消えて見えるようになる）
        if (IsAttackAnime)
        {
            var shot_ = Instantiate(_bolloonPrefab).GetComponent<Transform>();
            var icon_ = Instantiate(_animeIconPrefab).GetComponent<Transform>();

            shot_.parent = GameObject.Find(_shotManagerName).transform;
            icon_.parent = GameObject.Find(_shotManagerName).transform;
        }
        if (IsAttackIdol)
        {
            var shot_ = Instantiate(_bolloonPrefab).GetComponent<Transform>();
            var icon_ = Instantiate(_idolIconPrefab).GetComponent<Transform>();

            shot_.parent = GameObject.Find(_shotManagerName).transform;
            icon_.parent = GameObject.Find(_shotManagerName).transform;
        }
        if (IsAttackGame)
        {
            var shot_ = Instantiate(_bolloonPrefab).GetComponent<Transform>();
            var icon_ = Instantiate(_gameIconPrefab).GetComponent<Transform>();

            shot_.parent = GameObject.Find(_shotManagerName).transform;
            icon_.parent = GameObject.Find(_shotManagerName).transform;
        }
        if (IsAttackVocalRob)
        {
            var shot_ = Instantiate(_bolloonPrefab).GetComponent<Transform>();
            var icon_ = Instantiate(_vocalRobIconPrefab).GetComponent<Transform>();

            shot_.parent = GameObject.Find(_shotManagerName).transform;
            icon_.parent = GameObject.Find(_shotManagerName).transform;
        }
        if (IsAttackRobot)
        {
            var shot_ = Instantiate(_bolloonPrefab).GetComponent<Transform>();
            var icon_ = Instantiate(_robotIconPrefab).GetComponent<Transform>();

            shot_.parent = GameObject.Find(_shotManagerName).transform;
            icon_.parent = GameObject.Find(_shotManagerName).transform;
        }
    }
}