using UnityEngine;
using UnityEngine.UI;

public class BossHpBar : MonoBehaviour
{
    public Slider progressBar;
    public BossHpCounter bossHpCounter;

    void Update()
    {
        if (bossHpCounter != null && progressBar != null)
        {
            progressBar.value = bossHpCounter.CurrentBossDamageCount;
        }
    }
}