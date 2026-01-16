using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using Photon.Pun;

public class CircleMouse : MonoBehaviour
{
    BulletBtn bulletBtn;
    public Vector3 mousePosition;

    int shotNum = 1;                    // 총 탄막 개수(마우스 클릭 횟수) 제한

    public GameObject skillRange;        // 스킬 범위 최상위 오브젝트
    GameObject[] skillObject;            // 스킬 범위 오브젝트
    Vector3[] mouseLocation;             // 마우스 클릭 위치

    void Start()
    {
        if (PhotonNetwork.LocalPlayer.ActorNumber != GameObject.Find("RoomManager").GetComponent<Room>().bossActorNum)
        {
            gameObject.SetActive(false);
        }
        else
        {
            bulletBtn = GameObject.Find("BulletBtn").GetComponent<BulletBtn>();
        }

        skillObject = new GameObject[shotNum];
        mouseLocation = new Vector3[shotNum];
    }

    void Update()
    {
        if (PhotonNetwork.LocalPlayer.ActorNumber == GameObject.Find("RoomManager").GetComponent<Room>().bossActorNum)
        {
            if (bulletBtn.num == 0)
            {
                // 마우스 따라다니기
                gameObject.GetComponent<SpriteRenderer>().enabled = true;
                gameObject.transform.position = Camera.main.ScreenToWorldPoint(new Vector3(Input.mousePosition.x, Input.mousePosition.y, 1));

                if (Input.GetMouseButton(0))
                {
                    mousePosition = this.transform.position;

                    StartCoroutine(makeCircle());
                }
            }
            else
            {
                gameObject.GetComponent<SpriteRenderer>().enabled = false;
            }
        }
    }

    IEnumerator makeCircle()
    {
        for (int i = 0; i < shotNum; i++)
        {
            if (skillObject[i] == null)
            {
                // 스킬 위치 지정
                mouseLocation[i] = mousePosition;
                skillObject[i] = PhotonNetwork.Instantiate("FollowingMouse", mousePosition, Quaternion.identity);
                yield break;
            }
        }
    }
}