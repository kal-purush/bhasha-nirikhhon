using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;
using Photon.Pun;
using Photon.Realtime;

public class GameOverManager : MonoBehaviourPunCallbacks, IPunObservable
{

     // 현재 코드가 정상적인 실행을 막을 정도로 문제가 많기에
     // GameManager 오브젝트에서 잠시 분리했습니다 ㅠ.ㅠ


    public PhotonView pv;

    // 아래 두 변수(TimeLimit, Returncount)는 deltaTime이 정상적으로 작동하는 것을 확인할 경우 사용함
    private float TimeLimit = 50.0F;                // 제한시간 count
    private float Returncount = 5.0F;            // 게임 종료 후 룸으로 복귀하는 동안의 시간 cont

    private bool gameovercheck = false;     // 로비 복귀를 발동시키는 플래그 변수

    public Text Timertext;                  // 화면에 표시되는 제한시간 타이머 텍스트
    public Text Winningtext;                // 아르마딜로 or 보스 유저 승리 시 표시되는 텍스트
    public Text Returntext;               // "다시 룸으로 이동합니다..." 텍스트
    public GameObject BossWin;              // 보스 유저 승리 시 나타나는 이미지
    public GameObject ArmadilloWin;         // 아르마딜로 유저 승리 시 나타나는 이미지
 
    public void Awake()
    {
        PhotonNetwork.SendRate = 100;
        PhotonNetwork.SerializationRate = 100;
        PhotonNetwork.AutomaticallySyncScene = true;    // sync
    }


   
    // Start is called before the first frame update
    public void Start()
    {
        Time.timeScale = 1;                  // 게임 배속 초기화 
        TimeLimit = 50.0F;                  // 스테이지 제한시간은 50초
        Returncount = 5.0F;                 // 게임 종료 이미지를 표시하는 시간 5초

        if (SceneManager.GetActiveScene().name.Equals("Game Scene"))
        {
            if (PhotonNetwork.IsMasterClient)
            {
                pv.RPC("TimeCount", RpcTarget.All);     // 카운트 실행
            }
        }
    }

    public void Update()
    {
        if (SceneManager.GetActiveScene().name.Equals("Game Scene"))
        {
            if (PhotonNetwork.IsMasterClient)
            {
                // 모든 유저가 사망했을 경우
                if (GameObject.Find("RoomManager").GetComponent<Room>().checkBoss.Count == PhotonNetwork.PlayerList.Length + 1)
                {
                    pv.RPC("BossWinUI", RpcTarget.All);

                    PhotonNetwork.DestroyAll();
                    if (PhotonNetwork.InRoom)
                    {
                        PhotonNetwork.LeaveRoom();
                    }
                }
                // 제한시간이 모두 지남
                if (gameovercheck)
                {
                    PhotonNetwork.DestroyAll();
                    if (PhotonNetwork.InRoom)
                    {
                        PhotonNetwork.LeaveRoom();
                    }
                }
            }
        }
    }


    [PunRPC]
    void BossWinUI()
    {
        StartCoroutine(CoBossWin());
    }

    // 보스유저 승리화면 출력 코루틴
    IEnumerator CoBossWin()
    {
        Time.timeScale = 1;                                                  // 배속 원상복귀 (3번 탄막을 사용하고 있는 경우에 대응하여)
        Winningtext.text = "            유령 Win!\n[ 유령들이 방송국을 점거했다 ... ]";
        Returntext.text = "스테이지를 종료합니다...";
        BossWin.SetActive(true);
        yield return new WaitForSeconds(5f);                                 // 5초간 결과화면 표시

        yield return null;
    }

    // 게임 종료 + 결과 화면 출력 코루틴 호출 함수
    [PunRPC]
    void TimeCount()
    {
        StartCoroutine(CoTimeCount());
    }

    // 게임 종료 및 결과 화면 출력 코루틴
    IEnumerator CoTimeCount()
    {
        if (SceneManager.GetActiveScene().name.Equals("Game Scene"))
        {
            if (PhotonNetwork.IsMasterClient)
            {
                yield return new WaitForSeconds(50f);

                // 50초가 경과할 동안 다른 메서드(인원 수 부족, 모두 보스가 됨)가 실행되지 않은 경우
                // 아르마딜로 승리
                Time.timeScale = 1;                                          // 배속 원상복귀 (3번 탄막을 사용하고 있는 경우에 대응하여)
                ArmadilloWin.SetActive(true);
                Winningtext.text = "          아르마딜로 Win!\n[아르마딜로들이 방송국을 지켜냈다...]";
                Returntext.text = "스테이지를 종료합니다...";
                yield return new WaitForSeconds(5f);                        // 5초간 결과화면 표시

                gameovercheck = true;                                       // 로비 복귀를 위해 플래그를 true로 전환
            }
        }
        yield return null;
    }



    // 제한시간, 카운트다운 동기화
    public void OnPhotonSerializeView(PhotonStream stream, PhotonMessageInfo info)
    {
        // 통신 보내기
        if (stream.IsWriting)
        {
            stream.SendNext(TimeLimit);
            stream.SendNext(Returncount);
        }

        else
        {
            this.TimeLimit = (float)stream.ReceiveNext();
            this.Returncount = (float)stream.ReceiveNext();
        }
    }
}