using Sim.Scriptables;
using TMPro;
using UnityEngine;

public class PhoneArticleCardUI : MonoBehaviour {
    [Header("Settings")]
    [SerializeField]
    private TextMeshProUGUI nameTxt;

    [SerializeField]
    private TextMeshProUGUI categoryTxt;

    [SerializeField]
    private TextMeshProUGUI priceTxt;

    [SerializeField]
    private Transform colorContainer;

    [SerializeField]
    private SimpleColorButton simpleColorButtonPrefab;

    [SerializeField]
    private DoubleColorButton doubleColorButtonPrefab;

    [SerializeField]
    private TripleColorButton tripleColorButtonPrefab;

    public void Setup(PropsConfig config) {
        this.nameTxt.text = config.GetDisplayName();
        this.categoryTxt.text = "Furniture"; // TODO Change this
        this.priceTxt.text = "250";

        foreach (var configPreset in config.Presets) {
            ColorButton prefabToUse = this.simpleColorButtonPrefab;

            if (configPreset.Tertiary.Enabled) {
                prefabToUse = this.tripleColorButtonPrefab;
            } else if (configPreset.Secondary.Enabled) {
                prefabToUse = this.doubleColorButtonPrefab;
            }

            ColorButton colorButton = Instantiate(prefabToUse, this.colorContainer);
            colorButton.Setup(configPreset);
        }
    }
}