using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class N_CardDeckSys : MonoBehaviour
{

    public bool isTest;                  // 테스트 모드 : 카드 덱 상태를 Inspector창에서 설정
    public float SuffleTime = 20, BlinkTime = 5, DrawTime = 0.5f;
    public int MaxEnergy = 100;  // 에너지 최대치
    public int TotalCard;               // 총 카드 수
    public int[] CardOrder;           // 카드 순서 관리할 변수
    public Text CardNText, EnergyText;
    public Animator EnergyAnimator;
    private int CardN, CardIndex, Energy;     // 남은 카드 수, 에너지양
    public Image[] CardImage;
    public Sprite[] CardSprite;     // 카드 그래픽 (뒷면)
    public Sprite ClearSprite;      // 투명 그래픽 (for BlinkCards)

    // Start is called before the first frame update
    void Start()
    {
        // test상황이면 Inspector창에서 설정한 값으로 덱 상태 설정.
        // 아니면 N_PlayerInfo 스크립트 상에 설정된 값을 불러옴.
        if (!isTest)
        {
            TotalCard = N_PlayerInfo.CardSum;
            CardOrder = new int[TotalCard];
            // 카드를 차곡차곡 CardOrder에 쌓음.
            int index = 0;
            for (int i = 0; i < 9; i++)
            {
                for (int j = 0; j < N_PlayerInfo.CardNum[i]; j++)
                {
                    CardOrder[index++] = i;
                }
            }
        }
        else
        {
            if(CardOrder.Length != TotalCard)
            {
                print("Error(Inspector) : 카드 덱 설정 오류");
            }
        }

        CardN = TotalCard;
        CardNText.text = "" + CardN;
        CardIndex = 0;
        CardShuffle();

        StartCoroutine("CardDeckSystem");
        
    }

    IEnumerator CardDeckSystem()
    {
        // 변수 초기화
        float TimeToSleep = SuffleTime - BlinkTime - (DrawTime * 5) - 1;
        int index = 0;

        yield return new WaitForSecondsRealtime(1);

        // 카드덱 시스템
        while (true)
        {
            // 카드를 버린다.
            for (int i = 0; i < 5; i++)
            {
                // + 카드 버리는 애니메이션
                CardImage[i].sprite = ClearSprite;
            }
            yield return new WaitForSecondsRealtime(0.75f);
            EnergyAnimator.SetTrigger("Blink");
            yield return new WaitForSecondsRealtime(0.25f);

            // 에너지 회복
            Energy = MaxEnergy;
            EnergyText.text = "" + Energy;

            // 카드를 뽑는다.
            for (int i = 0; i < 5; i++)
            {
                if (CardN > 0)
                {
                    CardNText.text = "" + --CardN;
                    index = CardOrder[CardIndex++];
                    CardImage[i].sprite = CardSprite[index];
                    // + 카드 드로우 애니메이션
                    yield return new WaitForSecondsRealtime(DrawTime);
                }
                // 뽑을 카드가 없으면 덱을 루프시킨다.
                else
                {
                    // + 카드 루프 애니메이션
                    CardShuffle();
                    CardN = TotalCard;
                    CardNText.text = "" + CardN;
                    CardIndex = 0;
                    i--;
                    yield return new WaitForSecondsRealtime(DrawTime);
                    TimeToSleep -= DrawTime;
                }
            }

            yield return new WaitForSecondsRealtime(TimeToSleep);

            // BlinkTime만큼 깜박인다.
            for (int i = 1; i <= BlinkTime; i++)
            {
                for (int m = 0; m < 5; m++)
                {
                    CardImage[m].color = new Color(1, 1, 1, 0.3f);
                }
                yield return new WaitForSecondsRealtime(0.5f);
                for (int n = 0; n < 5; n++)
                {
                    CardImage[n].color = new Color(1, 1, 1, 1);
                }
                yield return new WaitForSecondsRealtime(0.5f);
            }
        }
    }

    // 카드를 섞는다.
    void CardShuffle()
    {
        for (int i = 0; i < TotalCard; i++)
        {
            int random = Random.Range(0, TotalCard);
            int temp = CardOrder[random];
            CardOrder[random] = CardOrder[i];
            CardOrder[i] = temp;
        }
    }

}