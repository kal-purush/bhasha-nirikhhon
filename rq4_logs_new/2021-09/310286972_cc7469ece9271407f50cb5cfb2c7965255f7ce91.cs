using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BattleBGMControll_Y : MonoBehaviour
{
    private CriAtomSource criAtomSource;
    bool battleFlg;
    List<GameObject> fightingEnemies;
    // Start is called before the first frame update
    void Start()
    {
        criAtomSource = GetComponent<CriAtomSource>();
        fightingEnemies = new List<GameObject>();
    }

    // Update is called once per frame
    void Update()
    {
        //Updateでの処理
        bool clearFlg = false;
        foreach (var e in fightingEnemies) if (e != null) clearFlg = true;
        if (clearFlg) battleFlg = false;
    }

    public void SetBattleBGM(GameObject enemy)
    {
        //すでにリストに追加済みの敵だったら無視
        foreach (var e in fightingEnemies) if (e == enemy) return;
        if (battleFlg) return;

        fightingEnemies.Clear();
        battleFlg = true;
        CriAtomSource criAtomSource = new CriAtomSource();
        criAtomSource.Play();
    }
}