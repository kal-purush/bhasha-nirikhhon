using TMPro;
using UnityEditor;
using UnityEngine;
using UnityEngine.EventSystems;
using UnityEngine.UI;

public class PopupInstruction : BasePopup
{
    [Header("Parameters")]
    [SerializeField] private Button m_ExitButton;
    [SerializeField] private TextMeshProUGUI m_ContentTxt;
    [SerializeField] private GlobalConfig globalConfig;

    //[Header("Scroll View Setting")]
    //[SerializeField] private RectTransform m_Content;
    //[SerializeField] private ScrollRect scrollRect;
    //[SerializeField] private Scrollbar verticalScrollbar;
    private void Start()
    {
        m_ContentTxt.text = globalConfig.instructionInfo;
        //scrollRect.onValueChanged.AddListener((pos) => ClampContent());
        //verticalScrollbar.onValueChanged.AddListener(value =>
        //{
            
        //    ClampContent();
        //});
        m_ExitButton.onClick.AddListener(OnClickExitButton);
    }

    //private void ClampContent()
    //{
    //    RectTransform rectTransform = scrollRect.GetComponent<RectTransform>();
    //    float curY = m_Content.anchoredPosition.y;
     
    //    m_ContentTxt.ForceMeshUpdate();
    //    // Công thức tính tọa độ tối đa được scroll thay đổi theo text
    //    float posY = m_ContentTxt.preferredHeight - m_ContentTxt.margin.y - rectTransform.rect.height + Mathf.Abs(rectTransform.anchoredPosition.y);
    //    curY = Mathf.Clamp(curY, 0, posY);
    //    m_Content.anchoredPosition = new Vector2(m_Content.anchoredPosition.x, curY);

    //    //float norm = posY <= 0 ? 1f : 1f - (curY / posY);

    //    //// Đồng bộ lại cho ScrollRect và Scrollbar
    //    //scrollRect.verticalNormalizedPosition = norm;
    //    //verticalScrollbar.value = norm;
    //}
    private void OnClickExitButton()
    {
        this.Hide();
    }


}