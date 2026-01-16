using UnityEngine;
using UnityEngine.UI;

public class InventoryPanelBehaviour : MonoBehaviour {

    private Image panelImage;
    [SerializeField]
    private GameObject body;

    private void Start()
    {
        panelImage = GetComponentInChildren<Image>();
        panelImage.enabled = false;
        body.SetActive(false);
    }


    void Update () {
        if (Input.GetKeyDown(KeyCode.I))
        {
            panelImage.enabled = !panelImage.enabled;
            body.SetActive(!body.activeSelf);
        }
	}
}