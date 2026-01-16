using System;
using DG.Tweening;
using TMPro;
using UnityEngine;
using UnityEngine.Serialization;
using UnityEngine.UI;

public class UISummonEffect: UIBase
{
        [SerializeField] private GameObject startEffect;
        [SerializeField] private GameObject summonEffect;
        [SerializeField] private GameObject summonResult;
        [SerializeField] private Image characterImage;
        [SerializeField] private TextMeshProUGUI resultText;
        [SerializeField] private Image goldImage;
        [SerializeField] private Animator animator;

        public void SetUp(SummonResult result,RectTransform pos)
        {
                Debug.Log(result.monster_id + " " + result.gold);
                summonEffect.SetActive(false);
                if (result.monster_id >= 0)
                {
                        characterImage.sprite = UIMonsterListManager.Instance.MonsterSlotsDictionary[result.monster_id].MonsterPropsSO.Icon;
                        characterImage.gameObject.SetActive(true);
                        goldImage.gameObject.SetActive(false);
                        resultText.text = UIMonsterListManager.Instance.MonsterSlotsDictionary[result.monster_id].MonsterPropsSO.MonsterData.Name;
                }
                else
                {
                        characterImage.gameObject.SetActive(false);
                        goldImage.gameObject.SetActive(true);
                        resultText.text = result.gold.ToString();
                }
                summonResult.SetActive(false);
                
                startEffect.SetActive(true);
                gameObject.GetComponent<RectTransform>()
                        .DOAnchorPos(pos.anchoredPosition, 1f)
                        .SetEase(Ease.OutQuad).OnComplete(() =>
                        {
                                startEffect.SetActive(false);
                                summonEffect.SetActive(true);
                        });
        }

        private void Update()
        {
                if (summonEffect.activeSelf)
                {
                        AnimatorStateInfo stateInfo = animator.GetCurrentAnimatorStateInfo(0);
                        if (stateInfo.normalizedTime >= 0.9f)
                        {
                                startEffect.SetActive(false);
                                summonEffect.SetActive(false);
                                summonResult.SetActive(true);
                                summonResult.GetComponent<RectTransform>().DOShakeAnchorPos(1.5f, new Vector2(10f, 10f), 1, 90, false, false)
                                        .SetLoops(-1, LoopType.Restart);
                        }
                }
        }

        private void OnDestroy()
        {
                DOTween.KillAll(startEffect);
                DOTween.KillAll(summonEffect);
                DOTween.KillAll(summonResult);
                DOTween.KillAll(gameObject);
        }
}