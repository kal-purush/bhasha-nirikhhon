using UnityEngine;
using System.Collections;
using UnityEngine.UI;

public class UpgradeBehaviour : MonoBehaviour
{
    public Text upgradeText;
    public GameObject upgradeButton;
    
    public GameObject CurrentPlant
    {
        set {
            PlantData monsterData = value.GetComponent<PlantData>();
            nextLevel = monsterData.getNextLevel();

            upgradeText.GetComponent<Text>().text = getNextLevelInfo();
            if (nextLevel == null)
            {
                // Hide upgrade button when level already max
                upgradeButton.SetActive(false);
            }
        }
    }

    public delegate void PlantUpgraded();
    public PlantUpgraded OnPlantUpgraded;

    public delegate void PlantSold();
    public PlantSold OnPlantSold;

    private PlantLevel nextLevel;    
    private GameManagerBehavior gameManager;

    // Use this for initialization
    void Start()
    {
        gameManager = GameObject.Find("GameManager").GetComponent<GameManagerBehavior>();
    }

    // Update is called once per frame
    void Update()
    {

    }

    private string getNextLevelInfo()
    {
        if (nextLevel != null)
        {
            return "Next level\nDamage: 20\nFire rate: " + (nextLevel.fireRate * 10)
            + "\nCost: " + nextLevel.cost;
        }
        return "Max level.";
    }

    public void UpgradePlant()
    {
        if (OnPlantUpgraded != null)
        {
            OnPlantUpgraded();
        }
        // Close upgrade menu
        gameObject.SetActive(false);
    }

    public void SellPlant()
    {
        if (OnPlantSold != null)
        {
            OnPlantSold();
        }
        gameObject.SetActive(false);
    }
}