using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using Cysharp.Threading.Tasks;

public class SavedListWindow : MonoBehaviour, IWindow
{
    public GameObject pregenerateUI;
    public GameObject viewerUI;

    [Header("Pre-Generate UI")]
    public RectTransform viewport;
    public GameObject loadingPanel;
    public GameObject showFormulaButton;

    [Header("Viewer UI")]
    public Text formulaText;
    public GameObject detailInfoPanel;
    public GameObject smilesViewer;
    private GameObject activeSmilesViewer;

    #region CONSTANT
    private const string AI_SERVER_HOST = "";
    #endregion

    private void Start()
    {
        Init();
        Generate();
    }

    public void Init()
    {
        pregenerateUI.SetActive(true);
        viewerUI.SetActive(false);
    }

    private void Generate()
    {
        loadingPanel.SetActive(true);

        for (int i = 0; i < UserDataManager.Instance.savedFormulaList.Count; i++)
        {
            var button = Instantiate(showFormulaButton, viewport).GetComponent<FormulaButtonBehaviour>();
            button.Init(UserDataManager.Instance.savedFormulaList[i], this);
            button.GetComponent<RectTransform>().anchoredPosition = new Vector2(0, -60 * (i + 1));
        }

        loadingPanel.SetActive(false);
    }

    public void OpenViewer(string formula = "")
    {
        formulaText.text = formula;
        activeSmilesViewer = Instantiate(smilesViewer);

        pregenerateUI.SetActive(false);
        viewerUI.SetActive(true);
    }

    public void ToggleDetailInfoPanel()
    {
        detailInfoPanel.SetActive(!detailInfoPanel.activeSelf);
    }

    public void Save()
    {
        UserDataManager.Instance.AddFormula(SmilesParseEngine.formula);
    }

    public void CloseViewer()
    {
        if (activeSmilesViewer != null)
            Destroy(activeSmilesViewer);

        formulaText.text = string.Empty;

        Init();
    }

    private void OnDestroy()
    {
        CloseViewer();
    }
}