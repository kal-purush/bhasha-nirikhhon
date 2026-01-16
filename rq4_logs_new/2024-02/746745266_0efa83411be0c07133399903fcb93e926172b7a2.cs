using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class W1 : Singleton<W1>
{
    [SerializeField] private Button equipBtn;
    [SerializeField] private Button equipedBtn;
    [SerializeField] private Button buyBtn;

    void Start()
    {
        equipBtn.onClick.AddListener(OnClickEquipBtn);
        buyBtn.onClick.AddListener(OnClickBuyBtn);
    }

    public void OnClickBuyBtn()
    {
        if (DataManager.Instance.dataDynamic.CurrentDynament >= 150)
        {
            DataManager.Instance.dataDynamic.CurrentDynament -= 150;
            UIManager.Instance.UpdateScoreDyamon();
            buyBtn.gameObject.SetActive(false);
            equipBtn.gameObject.SetActive(true);
        }
    }

    public void OnClickEquipBtn()
    {
        equipBtn.gameObject.SetActive(false);
        equipedBtn.gameObject.SetActive(true);
        ThemeMangaer.Instance.ChangeTheme(0);
        W2.Instance.SetEquipBtn();
        W3.Instance.SetEquipBtn();
        W4.Instance.SetEquipBtn();
    }

    public void SetEquipBtn()
    {
        if (!buyBtn.gameObject.activeInHierarchy && equipedBtn.gameObject.activeInHierarchy)
        {
            equipBtn.gameObject.SetActive(true);
            equipedBtn.gameObject.SetActive(false);
        }
    }

    public void SetEquipedBtn()
    {
        equipBtn.gameObject.SetActive(false);
        equipedBtn.gameObject.SetActive(true);
    }
}