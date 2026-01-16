using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class FinishMotionK : MonoBehaviour
{
    public GameObject boss;
    public GameObject kamen;
    [SerializeField] float stageHeight;

    /*
    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {

    }
    */

    public void BossDown()
    {
        Vector3 bossPos;
        bossPos = new Vector3(boss.transform.position.x, stageHeight, boss.transform.position.z);
        kamen.transform.position = bossPos;
        Vector3 bossRot;
        bossRot = boss.transform.localEulerAngles;
        kamen.transform.rotation = Quaternion.Euler(0.0f, bossRot.y, 0.0f);
        kamen.SetActive(true);
    }
}