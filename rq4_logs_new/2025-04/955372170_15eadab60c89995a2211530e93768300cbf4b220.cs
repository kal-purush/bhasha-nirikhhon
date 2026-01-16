using UnityEngine;
using UnityEngine.UI;
using TMPro;
public class UIScriptCoins: MonoBehaviour
{
    public TextMeshProUGUI coinsUI;
    private int coinValue;

    void Update()
    {
        coinValue=GameManager.Instance.coins;
        coinsUI.text= coinValue.ToString();
        //lifeValue.ToString();
    }
}