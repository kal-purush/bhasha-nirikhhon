using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class BossBreathEvent : MonoBehaviour
{
    public GameObject BossDonguri;
    BossDonguriMove bossMove;
    public bool IsBreath = false;
    public bool stopBreath = false;
    public bool finishBreath = false;
    // Start is called before the first frame update
    void Start()
    {
        bossMove = BossDonguri.GetComponent<BossDonguriMove>();
    }

    // Update is called once per frame
    void Update()
    {
        if (bossMove.selectAttack == 0)
        {
            IsBreath = false;
            stopBreath = false;
            finishBreath = false;
        }
    }

    void startBreath()
    {
        IsBreath = true;
    }
    void StopBreath()
    {
        stopBreath = true;
    }

    void FinishBreath()
    {
        finishBreath = true;
    }
}