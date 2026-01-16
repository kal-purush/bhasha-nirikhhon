using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using Photon.Pun;

public class LaserOn : MonoBehaviour
{
    /*
        실제 조명 스프라이트 (피격 판정되는 스프라이트)의 깜빡임을 제어하는 코드
        Range가 2초동안 FadeIn 될 때까지 대기하다가, 1초간 투명도를 높여 표시시킨 뒤
        다시 원래 위치(벽 쪽)로 이동시킨 뒤 투명하게 전환시키는 코드
    */

    BulletBtn bulletBtn;  // 탄막 버튼 스크립트
    public PhotonView pv;
    public GameObject LaserOnManager;

    private bool check = true;
    public float timer;
    public int waitingTime;

    SpriteRenderer rangesr;

    [PunRPC]
    public void Laser()
    {
        StartCoroutine(CoLaser());
    }

    IEnumerator CoLaser()
    {
        check = false;
        yield return new WaitForSeconds(2);                 // FadeIn 2초간 대기

        rangesr = this.gameObject.GetComponent<SpriteRenderer>();
        Color tempColor = rangesr.color;
        rangesr.color = tempColor;

        // 레이저 표시
        tempColor.a = 1f;
        rangesr.color = tempColor;
        yield return new WaitForSeconds(1); //2초간 레이저 출력

        //이후 레이저 위치 원상복귀 및 투명하게 전환
        this.transform.rotation = new Quaternion(0, 0, 0, 0);
        tempColor.a = 0f;
        rangesr.color = tempColor;

        check = true;                           // 플래그를 true로 전환
    }

    void Start()
    {
        timer = 0.0F;
        waitingTime = 2;
        //SLRng = GameObject.Find("StageLightRange");
    }

    void Update()
    {
        if (SceneManager.GetActiveScene().name.Equals("Game Scene"))
        {
            if (GameObject.Find("RoomManager") != null)
            {
                if (PhotonNetwork.LocalPlayer.ActorNumber == GameObject.Find("RoomManager").GetComponent<Room>().bossActorNum)
                {
                    bulletBtn = GameObject.Find("BulletBtn").GetComponent<BulletBtn>();

                    if (bulletBtn)
                    {
                        if (Input.GetMouseButtonDown(0) && bulletBtn.num == 1 && check)
                        {
                            pv.RPC("Laser", RpcTarget.All);
                        }
                    }
                }
            }
        }
    }
}