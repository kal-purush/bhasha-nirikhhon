using System.Collections;
using System.Collections.Generic;
using UnityEngine;

/*
NPC가 생성될 때 상호작용 오브젝트도 생성 >> start 함수
메리와의 거리가 일정거리 안에 메리라는 오브젝트의 위치 m  위치하면 다이얼로그 오브젝트 띄움 >> update 함수, 
 npc의 x 좌표 n, 메리의 x좌표 m 이라고 할 때, npc 위치의 5이하로 접근하면 반응. 즉 n-5 < m < n+5 일 경우 오브젝트 obj.active = true;
*/
public class NPC : MonoBehaviour
{
    public GameObject rose;
    public GameObject Mary;
    //private void ontriggerenter2d(Collider2D col)
    //{
    //    if (col.gameObject.tag == "mary")
    //    {
    //        Debug.Log("trigger!");
    //        rose.SetActive(true);
    //    }
    //}

    // Start is called before the first frame update


    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
        float mary_pos = Mary.GetComponent<Transform>().position.x;
        float obj_pos = gameObject.GetComponent<Transform>().position.x;
        Debug.Log(mary_pos);
        if ( Mathf.Abs(mary_pos - obj_pos) < 4)   // 마사 근처에 가면 대화버튼이 뜨게 함
        {
            rose.SetActive(true);
        }
        else
        {
            rose.SetActive(false);
        }
    }


}