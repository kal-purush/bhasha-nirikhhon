using UnityEngine;
using UnityEngine.UI;

public class PopupSettingGame : BasePopup
{
    [Header("Button List")]
    [SerializeField] private Button m_ExitButton;
    [SerializeField] private Button m_ShopButton;
    [SerializeField] private Button m_MusicButton;
    [SerializeField] private Button m_LanguageButton;
    [SerializeField] private Button m_BgmButton;
    [SerializeField] private Button m_SfxButton;
    [SerializeField] private Button m_VibrationButton;

    private void Start()
    {
        m_ExitButton.onClick.AddListener(OnClickExitButton);
    }

    private void OnClickExitButton()
    {
        this.Hide();
    }
}