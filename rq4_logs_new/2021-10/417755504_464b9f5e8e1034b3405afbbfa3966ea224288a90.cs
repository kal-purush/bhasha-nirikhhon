using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class CheckPointScript : MonoBehaviour
{
    /// <summary>チェックポイントの番号</summary>
    [Header("チェックポイントの番号")]
    public int CheckPointNumber;
    /// <summary>OrbitManagementを呼び出し</summary>
    OrbitManagement Orbit_M;
    [Header("このスクリプトが付いているObjを入れる")]
    public GameObject Orbit_obj;
    void Start()
    {
        Orbit_M = FindObjectOfType<OrbitManagement>();
        Orbit_M.MaxCheckPoint +=1;
        Orbit_M.checkPointObj[CheckPointNumber] = Orbit_obj;
    }
    public void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.tag=="Player")
        {
            //チェックポイントの通り過ごし防止
            if (Orbit_M.num == CheckPointNumber-1)
            {
                Orbit_M.num = CheckPointNumber;
            }
            //全チェックポイントの通過判定
            if (Orbit_M.num == Orbit_M.MaxCheckPoint)
            {
                Orbit_M.allCheckPoint = true;
            }
        }
    }
}