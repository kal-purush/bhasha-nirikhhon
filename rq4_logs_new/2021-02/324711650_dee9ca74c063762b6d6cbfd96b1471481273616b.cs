using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FlashRange : MonoBehaviour
{
    /* 
    * [스킬 4번]과 관련된 코드입니다.
    * 스테이지 조명의 '조명 부분'의 점멸 범위 스프라이트를 제어하는 코드
    */

    BulletBtn bulletBtn;
    private bool check = true;                      // 중복 입력 방지를 위한 플래그
    SpriteRenderer rangesr;

    // 조명 점멸범위 페이드인 코루틴 호출 함수 
    public void FlashFadeIn(float fadeTime)
    {
        StartCoroutine(CoFlashFadeIn(fadeTime));
    }

    // 조명 점멸범위 페이드인 코루틴
    IEnumerator CoFlashFadeIn(float fadeTime)
    {
        check = false;                              // 플래그를 false로 전환
        int flashcount = 0;

        rangesr = this.gameObject.GetComponent<SpriteRenderer>();
        Color tempColor = rangesr.color;
        yield return new WaitForSeconds(5);
        while (flashcount < 2)
        {
            while (tempColor.a < 0.5f)
            {
                tempColor.a += Time.deltaTime / fadeTime;   // 알파값을 0.5까지 증가
                rangesr.color = tempColor;                  // 증가시킨 알파값 적용

                if (tempColor.a >= 0.5f) tempColor.a = 0.5f;
                yield return null;
            }
            rangesr.color = tempColor;
            tempColor.a = 0f;                       //스킬범위 투명화
            rangesr.color = tempColor;
            flashcount++;
            yield return new WaitForSeconds(5);
        }
        yield return new WaitForSeconds(1);         // 1초 delay
        check = true;                               // 플래그를 true로 전환
    }

    // Start is called before the first frame update
    void Start()
    {
        bulletBtn = GameObject.Find("BulletBtn").GetComponent<BulletBtn>();
    }

    void OnEnable()                                 // 스킬이 활성화 될 때
    {
        FlashFadeIn(2f);                            // 실행
    }

}