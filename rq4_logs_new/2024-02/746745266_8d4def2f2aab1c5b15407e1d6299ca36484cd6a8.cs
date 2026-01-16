using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class ShopManager : Singleton<ShopManager>
{
    [SerializeField] private Button ad120Btn;
    [SerializeField] private Button m1KBtn;
    [SerializeField] private Button m3KBtn;
    [SerializeField] private Button m5KBtn;
    [SerializeField] private Button m25KBtn;
    [SerializeField] private Button m50KBtn;

    [SerializeField] private TextMeshProUGUI diamondShopText;

    public TextMeshProUGUI DiamondShopText { get => diamondShopText; set => diamondShopText = value; }

    private void Start()
    {
        ad120Btn.onClick.AddListener(OnClickAd120Btn);
        m1KBtn.onClick.AddListener(OnClickAdm1KBtn);
        m3KBtn.onClick.AddListener(OnClickAdm3KBtn);
        m5KBtn.onClick.AddListener (OnClickAdm5KBtn);
        m25KBtn.onClick.AddListener(OnClickAdm25KBtn);
        m50KBtn.onClick.AddListener(OnClickAdm50KBtn);
    }

    public void OnClickAd120Btn()
    {
        DataManager.Instance.dataDynamic.CurrentDynament += 120;
        UIManager.Instance.UpdateScoreDyamon();
    }

    public void OnClickAdm1KBtn()
    {
        DataManager.Instance.dataDynamic.CurrentDynament += 1000;
        UIManager.Instance.UpdateScoreDyamon();
    }

    public void OnClickAdm3KBtn()
    {
        DataManager.Instance.dataDynamic.CurrentDynament += 3000;
        UIManager.Instance.UpdateScoreDyamon();
    }

    public void OnClickAdm5KBtn()
    {
        DataManager.Instance.dataDynamic.CurrentDynament += 5000;
        UIManager.Instance.UpdateScoreDyamon();
    }

    public void OnClickAdm25KBtn()
    {
        DataManager.Instance.dataDynamic.CurrentDynament += 25000;
        UIManager.Instance.UpdateScoreDyamon();
    }

    public void OnClickAdm50KBtn()
    {
        DataManager.Instance.dataDynamic.CurrentDynament += 50000;
        UIManager.Instance.UpdateScoreDyamon();
    }
}