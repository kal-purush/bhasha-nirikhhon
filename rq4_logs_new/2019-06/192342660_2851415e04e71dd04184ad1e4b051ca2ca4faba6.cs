using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
/*
움직이는 NPC에만 추가적으로 적용되는 script
초기위치와 타켓위치 사이의 거리(dist)를 지정해 NPC를 움직이게 한다.
     */
public class movingNPC : MonoBehaviour
{
    public float speed; // NPC 이동 속도
    private Vector3 initpos; // NPC의 초기 위치
    private Vector3 targetpos; // NPC를 이동시킬 위치
    public int dist;
    private bool direct = true; // NPC의 이동 방향

    // Start is called before the first frame update
    void Start()
    {
        initpos.x = transform.position.x;
        targetpos.x = initpos.x + dist;
    }

    // Update is called once per frame
    void Update()
    {
        if (EventSystem.current.IsPointerOverGameObject() == true) return; // UI창 나오면 안움직임

        
        float targetDis = targetpos.x - transform.position.x; // 타겟까지의 남은 거리
        float initDis = transform.position.x - initpos.x; // 시작 위치부터의 현위치까지의 거리
        if(targetDis<0 || initDis < 0)  // 원위치-타켓위치 범위를 벗어나면 방향을 바꿈
        {
            if (direct)
            {
                direct = false;
            }
            else
            {
                direct = true;
            }
        }
        if (direct)
        {
            transform.Translate(Vector3.right * speed * Time.deltaTime);
        }
        else
        {
            transform.Translate(Vector3.left * speed * Time.deltaTime);
        }
        
    }
}