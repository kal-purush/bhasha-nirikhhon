using System;
using System.Linq;
using DG.Tweening;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

public class BottomBarMediator : MonoBehaviour
{
    private UserModel userModel = UserModel.Instance;
    [SerializeField] private GameObject WalletPopup;
    [SerializeField] private GameObject giftCodeBtn_2;
    [SerializeField] private Button findBoardBtn, gifttCodeBtn, rankBtn, shopBtn;
    private DataPayReceived dataPayMod;
    private DataPayReceived dataPayWallet;
    [SerializeField] private TextMeshProUGUI txtKm;

    public static bool IsAndroid()
    {
        return Application.platform == RuntimePlatform.Android;
    }

    public static bool IsIOS()
    {
        return Application.platform == RuntimePlatform.IPhonePlayer;
    }

    public static bool IsWeb()
    {
        return Application.platform == RuntimePlatform.WebGLPlayer;
    }
    void Start()
    {
        //findBoardBtn.onClick.AddListener(OnFindBoard);
        shopBtn.onClick.AddListener(OnShopClick);
    }

    public void OnShopClick()
    {
        GameUtils.OnShopClick();
        WalletPopup.SetActive(true);
    }

        private void OnEnable()
    {
        Signals.Get<LogoutSignal>().AddListener(OnLogout);
#if UNITY_IOS
        iconKm.SetActive(false);
        if (giftCodeBtn != null) giftCodeBtn.gameObject.SetActive(false);
        if (giftCodeBtn_2 != null) giftCodeBtn_2.SetActive(false);
#else
        var km = UserModel.Instance.kmValue;
        var isNormalPlayer = GameModel.Instance.IsNormalPlayer() || GameUtils.IsWeb();
        //iconKm.SetActive(isNormalPlayer && !string.IsNullOrEmpty(km) && int.Parse(km) > 0);
        
        //if (giftCodeBtn != null) giftCodeBtn.gameObject.SetActive(isNormalPlayer);
        if (giftCodeBtn_2 != null) giftCodeBtn_2.gameObject.SetActive(isNormalPlayer);
#endif
        txtKm.text = $"{UserModel.Instance.kmValue} tỷ lệ";
    }

    private void OnDisable()
    {
        Signals.Get<LogoutSignal>().RemoveListener(OnLogout);
    }

        private void OnLogout()
    {
        dataPayMod = null;
        dataPayWallet = null;
    }

}