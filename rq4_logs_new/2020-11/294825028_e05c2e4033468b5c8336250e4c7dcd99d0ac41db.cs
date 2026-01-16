using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using TMPro;
public class HealthBar : MonoBehaviour
{
    [SerializeField] GameObject heartPrefab;

    List<Image> hearts = new List<Image>();

    int currentHp;

    // Start is called before the first frame update
    void Start()
    {

    }

    public void InitializeHealthBar(int maxHealth)
    {
        currentHp = maxHealth;
        int numHearts = maxHealth / 20;

        if(maxHealth % 20 != 0)
        {
            numHearts += 1;
        }

        for(int i = 0; i < numHearts; i++)
        {
            Image newHeart = Instantiate(heartPrefab, transform).GetComponent<Image>();
            newHeart.type = Image.Type.Filled;
            newHeart.fillMethod = Image.FillMethod.Radial360;
            newHeart.fillOrigin = 2;
            newHeart.fillClockwise = false;
            hearts.Add(newHeart);
        }

        UpdateHealth(currentHp);
    }

    public void UpdateHealth(int health)
    {
        currentHp = health;
        int fullHearts = currentHp % 20;

        int currentHeart = 0;

        ClearHealthBar();
        for (int i = currentHp; i > 0;)
        {
            if (i >= 20)
            {
                hearts[currentHeart].fillAmount = 1;
                currentHeart++;
                i -= 20;
            }

            if (i < 20)
            {
                hearts[currentHeart].fillAmount = ((float)i / 5) * (.25f);
                break;
            }

        }
    }

    void ClearHealthBar()
    {
        for(int i = 0; i < hearts.Count; i++)
        {
            hearts[i].fillAmount = 0;
        }
    }
}