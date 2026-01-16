using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;
using UnityEngine.UI;

public class PlayerLevelingUI : MonoBehaviour
{
    public GameObject levelingUI;
    public bool isLeveling;

    public int uiHealthLevel;
    public int uiStaminaLevel;
    public int uiDamageLevel;
    public int uiDexLevel;
    public int uiDiscoveryLevel;
    public int uiSoulsCount;

    public int uiHealthCoefficient;
    public int uiStaminaCoefficient;
    public int uiDamageCoefficient;
    public int uiDexCoefficient;
    public int uiDiscoveryCoefficient;

    public TextMeshProUGUI levelHealth;
    public TextMeshProUGUI levelStamina;
    public TextMeshProUGUI levelDamage;
    public TextMeshProUGUI levelDexterity;
    public TextMeshProUGUI levelLuck;
    public TextMeshProUGUI soulsCount;

    public TextMeshProUGUI levelHealthMod;
    public TextMeshProUGUI levelStaminaMod;
    public TextMeshProUGUI levelDamageMod;
    public TextMeshProUGUI levelDexterityMod;
    public TextMeshProUGUI levelLuckMod;


    private void Awake()
    {
        levelingUI.SetActive(false);
        isLeveling = false;
    }

    // Update is called once per frame
    void Update()
    {
        
    }

    public void OpenLevelingUI()
    {
        if (isLeveling == false)
        {
            isLeveling = true;
            levelingUI.SetActive(true);
            UpdatePlayerStatsUI();
            UpdatePlayerStatsDisplay();
            return;
        }
        else if (isLeveling == true)
        {
            isLeveling = false;
            levelingUI.SetActive(false);
            return;
        }


    }


    public void UpdatePlayerStatsUI()
    {
        uiHealthLevel = PlayerStats.Instance.levelHealth;
        uiStaminaLevel = PlayerStats.Instance.levelStamina;
        uiDamageLevel = PlayerStats.Instance.levelDamage;
        uiDexLevel = PlayerStats.Instance.levelDex;
        uiDiscoveryLevel = PlayerStats.Instance.levelDiscovery;
        uiSoulsCount = PlayerStats.Instance.soulsCount;

        uiHealthCoefficient = PlayerStats.Instance.levelHealthCoeffecient;
        uiStaminaCoefficient = PlayerStats.Instance.levelStaminaCoeffecient;
        uiDamageCoefficient = PlayerStats.Instance.levelDamageCoeffecient;
        uiDexCoefficient = PlayerStats.Instance.levelDexCoeffecient;
        uiDiscoveryCoefficient = PlayerStats.Instance.levelDiscoveryCoeffecient;
    }

    public void UpdatePlayerStatsDisplay()
    {
        levelHealth.text = uiHealthLevel.ToString();
        levelStamina.text = uiStaminaLevel.ToString();
        levelDamage.text = uiDamageLevel.ToString();
        levelDexterity.text = uiDexLevel.ToString();
        levelLuck.text = uiDiscoveryLevel.ToString();
        soulsCount.text = uiSoulsCount.ToString();

        levelHealthMod.text = "(" + uiHealthCoefficient + ")";
        levelStaminaMod.text = "(" + uiStaminaCoefficient + ")";
        levelDamageMod.text = "(" + uiDamageCoefficient + ")";
        levelDexterityMod.text = "(" + uiDexCoefficient + ")";
        levelLuckMod.text = "(" + uiDiscoveryCoefficient + ")";
    }


}