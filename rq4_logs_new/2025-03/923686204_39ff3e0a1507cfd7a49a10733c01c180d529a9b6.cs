using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using TMPro;

public class ShopManager : MonoBehaviour
{
    public static ShopManager Instance;

    [Header("UI Elements")]
    [SerializeField] private GameObject shopPanel;
    [SerializeField] private List<Button> buffButtons;
    [SerializeField] private List<TMP_Text> buffNames;
    [SerializeField] private List<TMP_Text> buffDescriptions;
    [SerializeField] private List<Image> buffIcons;
    // UI field for displaying buff cost.
    [SerializeField] private List<TMP_Text> buffCosts;
    [SerializeField] private TMP_Text currencyText;

    [Header("Buff Data")]
    [SerializeField] private List<BuffData> availableBuffs;
    private List<BuffData> generatedBuffs = new List<BuffData>();

    [Header("References")]
    [SerializeField] private BuffHandler buffHandler;
    private GameObject player;

    private void Awake()
    {
        if (Instance == null)
        {
            Instance = this;
        }
        else
        {
            Destroy(gameObject);
        }
    }

    private void Start()
    {
        shopPanel.SetActive(false);
        player = GameObject.FindGameObjectWithTag("Player");
        if (player != null)
        {
            buffHandler = player.GetComponent<BuffHandler>();
        }
        else
        {
            Debug.LogError("Player not found in scene. Ensure the Player tag is set correctly.");
        }
    }

    private void UpdateCurrencyText()
    {
        currencyText.text = "Currency: " + GameManager.Instance.Currency.Value.ToString("F0");
    }

    public void ShowShop()
    {
        generatedBuffs.Clear();
        List<BuffData> buffPool = new List<BuffData>(availableBuffs);
        int count = Mathf.Min(4, buffPool.Count);

        // Generate 4 unique buffs.
        for (int i = 0; i < count; i++)
        {
            int randomIndex = Random.Range(0, buffPool.Count);
            generatedBuffs.Add(buffPool[randomIndex]);
            buffPool.RemoveAt(randomIndex);
        }

        UpdateShopUI();
        shopPanel.SetActive(true);
    }

    private void UpdateShopUI()
    {
        UpdateCurrencyText();
        for (int i = 0; i < buffButtons.Count; i++)
        {
            if (i < generatedBuffs.Count)
            {
                BuffData buff = generatedBuffs[i];
                buffNames[i].text = buff.buffName;
                buffDescriptions[i].text = buff.description;
                buffIcons[i].sprite = buff.icon;
                buffCosts[i].text = "Cost: " + buff.price.ToString();

                int index = i;
                buffButtons[i].onClick.RemoveAllListeners();
                buffButtons[i].onClick.AddListener(() => PurchaseBuff(index));
                buffButtons[i].gameObject.SetActive(true);
            }
            else
            {
                buffButtons[i].gameObject.SetActive(false);
            }
        }
    }

    public void PurchaseBuff(int index)
    {
        if (index < 0 || index >= generatedBuffs.Count)
        {
            Debug.LogWarning("Invalid buff selection index.");
            return;
        }

        BuffData selectedBuff = generatedBuffs[index];

        if (GameManager.Instance.Currency.Value < selectedBuff.price)
        {
            Debug.Log("Not enough currency to purchase this buff.");
            return;
        }

        GameManager.Instance.Currency.Value -= selectedBuff.price;
        Debug.Log("Buff purchased! Remaining currency: " + GameManager.Instance.Currency.Value);

        BuffInfo buffInfo = new BuffInfo
        {
            buffData = selectedBuff,
            currentStack = 1,
            target = player,
            creator = player
        };

        buffHandler.AddBuff(buffInfo);

        generatedBuffs.RemoveAt(index);
        UpdateShopUI();
    }

    public void RerollShop()
    {
        int desiredCount = Mathf.Min(4, availableBuffs.Count);

        int missing = desiredCount - generatedBuffs.Count;
        if (missing > 0)
        {
            List<BuffData> pool = new List<BuffData>();
            foreach (BuffData buff in availableBuffs)
            {
                if (!generatedBuffs.Contains(buff))
                {
                    pool.Add(buff);
                }
            }

            for (int i = 0; i < missing; i++)
            {
                if (pool.Count == 0)
                    break;
                int randomIndex = Random.Range(0, pool.Count);
                generatedBuffs.Add(pool[randomIndex]);
                pool.RemoveAt(randomIndex);
            }
        }
        else
        {
            ShowShop();
            return;
        }
        UpdateShopUI();
    }

    public void CloseShop()
    {
        shopPanel.SetActive(false);
    }
}