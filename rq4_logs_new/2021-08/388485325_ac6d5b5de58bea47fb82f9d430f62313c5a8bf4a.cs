using System;
using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.Events;
using Image = UnityEngine.UI.Image;

public class UIManager : MonoBehaviour
{
    [SerializeField] private Canvas UICanvas;
    
    //SM = Soul Meter
    [SerializeField] private float smFullTopPosY = -10;
    [SerializeField] private float smFullBotPosY = 120;
    
    [SerializeField] private float smFillTopPosY = 0;
    [SerializeField] private float smFillBotPosY = -130;

    [SerializeField] private Player player;

    [SerializeField] private RectTransform soulMeterFull;
    [SerializeField] private RectTransform soulMeterFill;

    [SerializeField] private float SMFillPos = 0;
    [SerializeField] private float SMFullPos = 0;

    [SerializeField] private GameObject healthUnit;
    [SerializeField] private Vector3 healthUnitPos0 = new Vector3(173, -52, 0);
    [SerializeField] private float unitShift = 60;
    [SerializeField] private Sprite healthFull;
    [SerializeField] private Sprite healthEmpty;

    [SerializeField] private GameObject[] healthUnits;
    private Image[] healthUnitsImage;

    [SerializeField] private TextMeshProUGUI tmpMoney;

    public static UnityAction m_SoulChange;
    public static UnityAction m_HealthChange;
    public static UnityAction m_MoneyChange;

    // Start is called before the first frame update
    void Start()
    {
        m_SoulChange += SetSoulMeter;
        m_SoulChange();

        m_HealthChange += SetHealthUnits;

        m_MoneyChange += SetMoney;
        m_MoneyChange();

        healthUnits = new GameObject[player.hpMax];
        healthUnitsImage = new Image[healthUnits.Length];
        for (int i = 0; i < healthUnits.Length; i++)
        {
            healthUnits[i] = Instantiate(healthUnit);
            healthUnits[i].transform.SetParent(UICanvas.transform, false);
            healthUnits[i].GetComponent<RectTransform>().anchoredPosition = new Vector2(healthUnitPos0.x + (i * unitShift), healthUnitPos0.y);

            healthUnitsImage[i] = healthUnits[i].GetComponent<Image>();
        }
    }

    //For testing purposes only, remove in web build.
    //TODO: Remove
    private void Update()
    {
        SetSoulMeter();
        SetHealthUnits();
        SetMoney();
    }

    public void SetSoulMeter()
    {
        var t = player.soul / 99f;

        SMFillPos = Mathf.Lerp(smFillBotPosY, smFillTopPosY, t);
        SMFullPos = Mathf.Lerp(smFullBotPosY, smFullTopPosY, t);

        soulMeterFill.anchoredPosition = new Vector3(0, Mathf.Lerp(smFillBotPosY, smFillTopPosY, t), 0);
        soulMeterFull.anchoredPosition = new Vector3(0, Mathf.Lerp(smFullBotPosY, smFullTopPosY, t), 0);
    }

    public void SetHealthUnits()
    {
        for (int i = 0; i < healthUnits.Length; i++)
        {
            if (i >= player.Hp)
            {
                healthUnitsImage[i].sprite = healthEmpty;
            }
            else
            {
                healthUnitsImage[i].sprite = healthFull;
            }
        }
    }

    public void SetMoney()
    {
        tmpMoney.text = player.money.ToString();
    }
}