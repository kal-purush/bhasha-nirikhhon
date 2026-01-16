using UnityEngine;
using UnityEngine.UI;

public class BuildingMenuButtonManagment : MonoBehaviour
    {
    [SerializeField]
    public GameObject buttonPrefab;
    public GameObject panel;
    public GameObject[] aktiveButtons;
    public int buttonId;
    private Quaternion quaternion;

    private void Start()
        {
        Invoke("CreateButtons", 0.1f );
        }
    public void CreateButtons()
        {
        aktiveButtons = new GameObject[BuildingSystem.buildingDirectory.Count];
        for (int i = 0; i < BuildingSystem.buildingDirectory.Count; i++)
            {
            Vector3 localPosition = new Vector3(100f * i , this.transform.parent.localPosition.y + 170f);
            Instantiate(buttonPrefab, localPosition , quaternion , panel.transform);
            buttonPrefab.GetComponent<BuildingMenuToggle>().panel = this.panel;
            buttonPrefab.GetComponent<ButtonManagment>().buttonId = buttonId;
            buttonPrefab.GetComponent<ButtonManagment>().objectToBuild = BuildingSystem.buildingDirectory[i];
            buttonId++;
            aktiveButtons[i] = buttonPrefab;
            }
        }

    }