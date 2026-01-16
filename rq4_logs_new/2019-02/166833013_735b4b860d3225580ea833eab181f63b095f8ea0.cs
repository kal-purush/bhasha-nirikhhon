using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class G_CardUI : MonoBehaviour
{
    public GameObject[] CardUI;
    public GameObject ErrorScreen;
    public Image ErrorImage;
    public Text GoldAmount;
    public Text CardSum;
    public Text[] CardText;
    public int Cardnum; //ActiveUI함수에서 카드번호 받아올 변수
    public int[] CardPrice = { 400, 350, 300, 250, 200, 150, 100 }; //상점 카드 가격


    // Start is called before the first frame update
    void Start()
    {
        GoldAmount.text = "" + N_PlayerInfo.Gold;
        CardSum.text = "" + N_PlayerInfo.CardSum;
        if (CardText.Length == 9)
        {
            for (int i = 0; i < CardText.Length; i++)
            {
                CardText[i].text = "X " + N_PlayerInfo.CardNum[i];
            }
        }
        else if (CardText.Length == 7)
        {
            CardText[0].text = "$ " + CardPrice[0];
            CardText[1].text = "$ " + CardPrice[1];
            CardText[2].text = "$ " + CardPrice[2];
            CardText[3].text = "$ " + CardPrice[3];
            CardText[4].text = "$ " + CardPrice[4];
            CardText[5].text = "$ " + CardPrice[5];
            CardText[6].text = "$ " + CardPrice[6];
        }
        else if (CardText.Length == 3)
        {
            for (int i = 0; i < CardText.Length; i++)
            {
                CardText[i].text = "X " + N_PlayerInfo.CardNum[i];
            }
        }
    }

    public void ActiveUI(int num)
    {
        Cardnum = num;
        for (int i = 0; i < CardUI.Length; i++)
        {
            CardUI[i].SetActive(false);
        }

        CardUI[num].SetActive(true);
    }

    /*Store_1 씬에서 카드 제거할 때 실행*/
    public void YesNo(int num)
    {
        print(Cardnum);
        CardUI[Cardnum].SetActive(false);
        if (num == 1) //yes 눌렀을 때 => 카드 제거해야함
        {
            //제거할 카드가 있는지 확인
            if (N_PlayerInfo.CardSum > 5 && N_PlayerInfo.CardNum[Cardnum] > 0)
            {
                if (N_PlayerInfo.Gold >= 500) //제거할 골드 있는지 확인
                {
                    N_PlayerInfo.Gold -= 500;
                    GoldAmount.text = "" + N_PlayerInfo.Gold;
                    N_PlayerInfo.CardSum--;
                    N_PlayerInfo.CardNum[Cardnum]--;
                    CardText[Cardnum].text = "X " + N_PlayerInfo.CardNum[Cardnum];
                    CardSum.text = "" + N_PlayerInfo.CardSum;
                }
                else //골드가 부족합니다.
                {
                    StartCoroutine("Error");
                }
            }
        }
    }

    /*Store_2 씬에서 카드 구입할 때 실행*/
    public void Buy(int num)
    {
        CardUI[Cardnum].SetActive(false);
        GoldAmount.text = "" + N_PlayerInfo.Gold;
        CardSum.text = "" + N_PlayerInfo.CardSum;
        if (num == 1) //yes 눌렀을 때 => 카드 구매해야함
        {
            if (N_PlayerInfo.Gold >= CardPrice[Cardnum]) //Gold 있는지 확인
            {
                N_PlayerInfo.Gold -= CardPrice[Cardnum]; //Gold 깎이고
                GoldAmount.text = "" + N_PlayerInfo.Gold;
                switch (Cardnum)
                {//카드 개수 증가
                    case 0: //wild카드
                        N_PlayerInfo.CardNum[3]++;
                        N_PlayerInfo.CardSum++;
                        CardSum.text = "" + N_PlayerInfo.CardSum;
                        break;
                    case 1: //도발카드
                        N_PlayerInfo.CardNum[4]++;
                        N_PlayerInfo.CardSum++;
                        CardSum.text = "" + N_PlayerInfo.CardSum;
                        break;
                    case 2: //허수아비카드
                        N_PlayerInfo.CardNum[5]++;
                        N_PlayerInfo.CardSum++;
                        CardSum.text = "" + N_PlayerInfo.CardSum;
                        break;
                    case 3: //벽설치_ㅣ카드
                        N_PlayerInfo.CardNum[0]++;
                        N_PlayerInfo.CardSum++;
                        CardSum.text = "" + N_PlayerInfo.CardSum;
                        break;
                    case 4: //뇌물카드
                        N_PlayerInfo.CardNum[6]++;
                        N_PlayerInfo.CardSum++;
                        CardSum.text = "" + N_PlayerInfo.CardSum;
                        break;
                    case 5: //비둘기전갈카드
                        N_PlayerInfo.CardNum[7]++;
                        N_PlayerInfo.CardSum++;
                        CardSum.text = "" + N_PlayerInfo.CardSum;
                        break;
                    case 6: //벽제거카드
                        N_PlayerInfo.CardNum[8]++;
                        N_PlayerInfo.CardSum++;
                        CardSum.text = "" + N_PlayerInfo.CardSum;
                        break;
                }
            }
            else if (num == -1)
            {
                for (int i = 0; i < CardUI.Length; i++)
                {
                    CardUI[i].SetActive(false);
                }
            }
            else //골드가 부족합니다.
            {
                StartCoroutine("Error");
            }
        }
    }

    /*Store_3 씬에서 카드 강화할 때 실행*/
    public void Upgrade(int num)
    {
        GoldAmount.text = "" + N_PlayerInfo.Gold;
        CardSum.text = "" + N_PlayerInfo.CardSum;
        if (num == 1)
        {
            // 골드가 남아있는지, 강화할 카드가 있는지 확인
            if (N_PlayerInfo.Gold >= 300 && N_PlayerInfo.CardNum[0] > 0)
            {
                N_PlayerInfo.Gold -= 300; //Gold 깎이고
                GoldAmount.text = "" + N_PlayerInfo.Gold;
                N_PlayerInfo.CardNum[0]--; //벽설치_ㅣ카드 감소
                CardText[0].text = "X " + N_PlayerInfo.CardNum[0];
                N_PlayerInfo.CardNum[2]++; //벽설치_ㅏ카드 증가
                CardText[1].text = "X " + N_PlayerInfo.CardNum[2];
                CardUI[0].SetActive(false);
            }
            else //골드가 부족합니다.
            {
                CardUI[0].SetActive(false);
                StartCoroutine("Error");
            }
        }
        else if (num == 2)
        {
            // 골드가 남아있는지, 강화할 카드가 있는지 확인
            if (N_PlayerInfo.Gold >= 400 && N_PlayerInfo.CardNum[2] > 0)
            {
                N_PlayerInfo.Gold -= 400; //Gold 깎이고
                GoldAmount.text = "" + N_PlayerInfo.Gold;
                N_PlayerInfo.CardNum[2]--; //벽설치_ㅏ카드 감소
                CardText[1].text = "X " + N_PlayerInfo.CardNum[2];
                N_PlayerInfo.CardNum[1]++; //벽설치_ㄴ카드 증가
                CardText[2].text = "X " + N_PlayerInfo.CardNum[1];
                CardUI[1].SetActive(false);
            }
            else //골드가 부족합니다.
            {
                CardUI[1].SetActive(false);
                StartCoroutine("Error");
            }
        }
        else if (num == 0)
        {
            for (int i = 0; i < CardUI.Length; i++)
            {
                CardUI[i].SetActive(false);
            }
        }
    }

    // 골드 부족할 때
    IEnumerator Error()
    {
        float valueA = 1;
        ErrorScreen.SetActive(true);
        for (int i = 1; i <= 10; i++)
        {
            yield return new WaitForSeconds(0.1f);
            valueA -= 0.1f;
            ErrorImage.color = new Color(1, 1, 1, valueA);
        }
        ErrorScreen.SetActive(false);
    }
}