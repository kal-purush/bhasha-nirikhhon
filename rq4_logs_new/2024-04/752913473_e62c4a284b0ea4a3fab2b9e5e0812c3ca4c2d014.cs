using System;
using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;

public class UpgradableTower : MonoBehaviour
{
    public enum UpgradeType
    {
        FireRate,
        Damage,
        Radius
    }

    [Serializable]
    public class KeyValuePair<TKey, TValue>
    {
        public TKey key;
        public TValue value;
    }

    [Serializable]
    public class Upgrade
    {
        public int cost;
        public float value;

        public Upgrade(int cost, float value)
        {
            this.cost = cost;
            this.value = value;
        }

        public int intValue => (int)value;
    }

    public AudioClip upgradeSound;
    public GameObject upgradePanelPrefab;
    public GameObject radiusPreviewPrefab;

    public List<KeyValuePair<UpgradeType, Upgrade[]>> upgradeValueList = new()
    {
        new() {
            key = UpgradeType.FireRate,
            value = new Upgrade[]
            {
                new(0, 1.0f), // Initial
                new(10, 0.9f),
                new(15, 0.8f),
                new(20, 0.7f),
            }
        },
        new() {
            key = UpgradeType.Damage,
            value = new Upgrade[]
            {
                new(0, 1), // Initial
                new(20, 2),
                new(30, 3),
                new(50, 4)
            }
        },
        new() {
            key = UpgradeType.Radius,
            value = new Upgrade[]
            {
                new(0, 8), // Initial
                new(10, 10)
            }
        }
    };

    SortedDictionary<UpgradeType, Upgrade[]> upgradeValues = new();

    SortedDictionary<UpgradeType, int> upgradeLevels = new();

    GameObject upgradesPanel;
    ShootEnemies shootEnemies;
    MoneyManager money;
    LevelManager levelManager;

    List<GameObject> tempObjects = new();

    void Start()
    {
        levelManager = FindObjectOfType<LevelManager>();
        upgradesPanel = levelManager.upgradesPanel;
        shootEnemies = GetComponent<ShootEnemies>();
        money = GameObject.FindGameObjectWithTag("Player").GetComponent<MoneyManager>();
        foreach (KeyValuePair<UpgradeType, Upgrade[]> upgrade in upgradeValueList)
        {
            upgradeValues[upgrade.key] = upgrade.value;
            upgradeLevels[upgrade.key] = 0;
            DoUpgrade(upgrade.key, 0);
        }
    }

    void Update()
    {
        foreach (UpgradeType upgradeType in Enum.GetValues(typeof(UpgradeType)))
        {
            if (Input.GetKeyDown(Key(upgradeType)))
            {
                DoUpgrade(upgradeType, upgradeLevels[upgradeType] + 1);
                SetupMenu();
                ShowRadius();
                return;
            }
        }
    }

    void SetupMenu()
    {
        upgradesPanel.SetActive(true);
        var upgrades = upgradesPanel.transform.Find("Upgrades");
        foreach (Transform child in upgrades)
        {
            Destroy(child.gameObject);
        }

        foreach (UpgradeType upgradeType in upgradeLevels.Keys)
        {
            MakePanel(upgradeType, upgrades);
        }
    }

    GameObject MakePanel(UpgradeType upgradeType, Transform parent)
    {
        int curLevel = upgradeLevels[upgradeType];
        Upgrade[] upgrades = upgradeValues[upgradeType];
        if (curLevel >= upgrades.Length - 1)
        {
            return null;
        }

        GameObject panel = Instantiate(upgradePanelPrefab, parent, worldPositionStays: false);
        panel.transform.Find("UpgradeTitle").GetComponent<TextMeshProUGUI>().text =
            UpgradeTitle(upgradeType);
        panel.transform.Find("UpgradeCost").GetComponent<TextMeshProUGUI>().text =
            "$" + upgrades[curLevel + 1].cost.ToString();
        var equation = panel.transform.Find("Equation");
        equation.Find("Key").Find("KeyLabel").GetComponent<TextMeshProUGUI>().text =
            Key(upgradeType).ToString();
        equation.Find("OldValue").GetComponent<TextMeshProUGUI>().text =
            DisplayValue(upgradeType, curLevel);
        equation.Find("NewValue").GetComponent<TextMeshProUGUI>().text =
            DisplayValue(upgradeType, curLevel + 1);
        equation.Find("OldLevel").GetComponent<TextMeshProUGUI>().text =
            "lv " + (curLevel + 1);
        equation.Find("NewLevel").GetComponent<TextMeshProUGUI>().text =
            "lv " + (curLevel + 2);
        panel.name = upgradeType.ToString() + "Panel";
        return panel;
    }

    KeyCode Key(UpgradeType upgradeType)
    {
        return upgradeType switch
        {
            UpgradeType.FireRate => KeyCode.Z,
            UpgradeType.Damage => KeyCode.X,
            UpgradeType.Radius => KeyCode.C,
            _ => KeyCode.None,
        };
    }

    string UpgradeTitle(UpgradeType upgradeType)
    {
        return upgradeType switch
        {
            UpgradeType.FireRate => "Fire Rate",
            UpgradeType.Damage => "Damage",
            UpgradeType.Radius => "Radius",
            _ => "",
        };
    }

    string DisplayValue(UpgradeType upgradeType, int level)
    {
        Upgrade[] upgrades = upgradeValues[upgradeType];
        if (level >= upgrades.Length)
        {
            return "";
        }

        return upgradeType switch
        {
            UpgradeType.Damage or UpgradeType.Radius => upgrades[level].intValue.ToString(),
            _ => upgrades[level].value.ToString("0.00"),
        };
    }

    void DoUpgrade(UpgradeType upgradeType, int level)
    {
        Upgrade[] upgrades = upgradeValues[upgradeType];
        if (level >= upgrades.Length || !money.HasAtLeast(upgrades[level].cost))
        {
            return;
        }

        AudioSource.PlayClipAtPoint(upgradeSound, transform.position);
        money.AddMoney(-upgrades[level].cost);
        switch (upgradeType)
        {
            case UpgradeType.FireRate:
                shootEnemies.fireInterval = upgrades[level].value;
                break;
            case UpgradeType.Damage:
                shootEnemies.damage = upgrades[level].intValue;
                break;
            case UpgradeType.Radius:
                shootEnemies.radius = upgrades[level].intValue;
                break;
        }
        upgradeLevels[upgradeType] = level;
    }

    void RemoveRadius()
    {
        foreach (var obj in tempObjects)
        {
            Destroy(obj);
        }
    }

    void ShowRadius()
    {
        RemoveRadius();
        var curLevel = upgradeLevels[UpgradeType.Radius];
        var curRadius = shootEnemies.radius;
        var groundPos = transform.position;
        groundPos.y = 0;
        var radiusPreview1 = Instantiate(radiusPreviewPrefab, groundPos, Quaternion.identity);
        radiusPreview1.transform.localScale =
            new Vector3(curRadius * 2, 1, curRadius * 2);
        tempObjects.Add(radiusPreview1);
        if (curLevel < upgradeValues[UpgradeType.Radius].Length - 1)
        {
            var nextRadius = upgradeValues[UpgradeType.Radius][curLevel + 1].intValue;
            var radiusPreview2 = Instantiate(radiusPreviewPrefab, groundPos, Quaternion.identity);
            radiusPreview2.transform.localScale =
                new Vector3(nextRadius * 2, 1, nextRadius * 2);
            radiusPreview2.GetComponent<Renderer>().material.color = new Color(0, 1, 0, 0.25f);
            tempObjects.Add(radiusPreview2);
        }
    }

    void OnTriggerEnter(Collider other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            SetupMenu();            
            levelManager.SetIsPlaceTowerDisabled(true);
            ShowRadius();
        }
    }

    void OnTriggerExit(Collider other)
    {
        if (other.gameObject.CompareTag("Player"))
        {
            upgradesPanel.SetActive(false);
            levelManager.SetIsPlaceTowerDisabled(false);
            RemoveRadius();
        }
    }
}