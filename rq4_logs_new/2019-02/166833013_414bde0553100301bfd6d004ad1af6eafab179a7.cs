using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class N_Scrow : MonoBehaviour
{
    public N_CardSystem NCS;

    public void OnTriggerEnter2D(Collider2D coll)
    {
        // 허수아비와 닿았다면
        if (coll.gameObject.CompareTag("scrow"))
        {
            print("허수아비 ==> 고양이");
            NCS.isScrowOn = true;
        }
    }

    public void OnTriggerExit2D(Collider2D coll)
    {
        // 허수아비가 나갔다면
        if (coll.gameObject.CompareTag("scrow"))
        {
            print("허수아비 =/=> 고양이");
            NCS.isScrowOn = false;
            // 도발상태였다면 도발 이미지로 변경
            if (NCS.isProvoke)
                NCS.graphic_change(2);
            else
                NCS.graphic_change(0);
        }
    }

}