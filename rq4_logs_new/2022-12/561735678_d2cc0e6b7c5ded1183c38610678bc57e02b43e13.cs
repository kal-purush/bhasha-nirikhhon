using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class MetaGameUI : MonoBehaviour
{
    private AbilityIconsList icons;

    private int activeSaveId;
    private Save activeSave;

    private GameObject metaGame;
    
    private GameObject characteristics;
    private Text healthText;
    private Text armorText;
    private Text experienceText;

    private Text nameLevel;

    private GameObject chooseAbilities;
    private GameObject remainingText;
    private Image ability0;
    private Image ability1;
    private Image ability2;

    private GameObject gainedAbilitites;
    private GameObject grid;
    private GameObject earnThemByFightingText;

    private bool generatedAbilities = false;
    private int ability0Id = 0;
    private int ability1Id = 0;
    private int ability2Id = 0;

    private List<int> addedAbilities;

    void Start()
    {
        addedAbilities = new List<int>();
        icons = GetComponent<AbilityIconsList>();
        activeSaveId = PlayerPrefs.GetInt("active");
        activeSave = JsonUtility.FromJson<Save>(PlayerPrefs.GetString("Save" + activeSaveId));

        activeSave = new Save();
        activeSave.playerFreeLevels = 10;
        activeSave.playerAbilities = new List<int>();

        getUIElements();

        if (activeSave != null)
        {
            changeUIElements();
        }
    }

    private void getUIElements()
    {
        metaGame = GameObject.Find("MetaGameUI");

        characteristics = metaGame.transform.Find("characteristics").gameObject;

        healthText = characteristics.transform.Find("HealthText").gameObject.GetComponent<Text>();
        armorText = characteristics.transform.Find("ArmorText").gameObject.GetComponent<Text>();
        experienceText = characteristics.transform.Find("ExperienceText").gameObject.GetComponent<Text>();

        nameLevel = metaGame.transform.Find("NameLevel").transform.Find("Text").GetComponent<Text>();

        chooseAbilities = metaGame.transform.Find("ChooseAbilities").gameObject;
        remainingText = chooseAbilities.transform.Find("RemainingText").gameObject;
        ability0 = chooseAbilities.transform.Find("ability0Button").GetComponent<Image>();
        ability1 = chooseAbilities.transform.Find("ability1Button").GetComponent<Image>();
        ability2 = chooseAbilities.transform.Find("ability2Button").GetComponent<Image>();

        gainedAbilitites = metaGame.transform.Find("GainedAbilities").gameObject;
        grid = gainedAbilitites.transform.Find("grid").gameObject;
        earnThemByFightingText = gainedAbilitites.transform.Find("EarnThemByFightingText").gameObject;
    }

    private void changeUIElements()
    {
        healthText.text = "" + activeSave.playerMaxHealth;
        armorText.text = "" + ((int)activeSave.playerArmor * 100) + "%";
        //...
    }

    // Update is called once per frame
    void Update()
    {
        showRemaining();
        showPlayerAbilities();
        generateAndShowAbilities();
    }

    private void generateAndShowAbilities()
    {
        if (activeSave == null)
        {
            ability0.gameObject.SetActive(false);
            ability1.gameObject.SetActive(false);
            ability2.gameObject.SetActive(false);
            return;
        }

        if (activeSave.playerFreeLevels > 0)
        {
            if (!generatedAbilities)
            {
                generatedAbilities = true;
                ability0Id = Random.Range(0, 25);
                ability1Id = Random.Range(0, 25);
                ability2Id = Random.Range(0, 25);

                ability0.sprite = icons.abilityIcons[ability0Id];
                ability1.sprite = icons.abilityIcons[ability1Id];
                ability2.sprite = icons.abilityIcons[ability2Id];
            }
        } else
        {
            ability0.gameObject.SetActive(false);
            ability1.gameObject.SetActive(false);
            ability2.gameObject.SetActive(false);
        }
    }

    private void showRemaining()
    {
        if (activeSave == null)
        {
            remainingText.GetComponent<Text>().text = "Remaining: 0 :(";
            return;
        }
        if (activeSave.playerFreeLevels == 0)
        {
            remainingText.GetComponent<Text>().text = "Remaining: 0 :(";
        } else
        {
            remainingText.GetComponent<Text>().text = "Remaining: " + activeSave.playerFreeLevels;
        }
    }

    private void showPlayerAbilities()
    {
        if (activeSave == null)
        {
            grid.SetActive(false);
            earnThemByFightingText.SetActive(true);
            return;
        }
        if (activeSave.playerAbilities.Count == 0)
        {
            grid.SetActive(false);
            earnThemByFightingText.SetActive(true);
        } else
        {
            earnThemByFightingText.SetActive(false);
            grid.SetActive(true);
        }
    }

    public void push0()
    {
        activeSave.playerFreeLevels--;
        generatedAbilities = false;

        activeSave.playerAbilities.Add(ability0Id);

        for (int i = 0; i < activeSave.playerAbilities.Count; i++)
        {
            var abilityId = activeSave.playerAbilities[i];
            var slot = grid.transform.Find("ability" + i).GetComponent<Image>();
            slot.sprite = icons.abilityIcons[abilityId];
            slot.gameObject.SetActive(true);
        }
        for (int i = activeSave.playerAbilities.Count; i < 24; i++)
        {
            grid.transform.Find("ability" + i).gameObject.SetActive(false);
        }
    }

    public void push1()
    {
        addedAbilities.Add(ability1Id);
        activeSave.playerFreeLevels--;
        generatedAbilities = false;

        activeSave.playerAbilities.Add(ability1Id);

        for (int i = 0; i < activeSave.playerAbilities.Count; i++)
        {
            var abilityId = activeSave.playerAbilities[i];
            var slot = grid.transform.Find("ability" + i).GetComponent<Image>();
            slot.sprite = icons.abilityIcons[abilityId];
            slot.gameObject.SetActive(true);
        }
        for (int i = activeSave.playerAbilities.Count; i < 24; i++)
        {
            grid.transform.Find("ability" + i).gameObject.SetActive(false);
        }
    }

    public void push2()
    {
        addedAbilities.Add(ability2Id);
        activeSave.playerFreeLevels--;
        generatedAbilities = false;

        activeSave.playerAbilities.Add(ability2Id);

        for (int i = 0; i < activeSave.playerAbilities.Count; i++)
        {
            var abilityId = activeSave.playerAbilities[i];
            var slot = grid.transform.Find("ability" + i).GetComponent<Image>();
            slot.sprite = icons.abilityIcons[abilityId];
            slot.gameObject.SetActive(true);
        }
        for (int i = activeSave.playerAbilities.Count; i < 24; i++)
        {
            grid.transform.Find("ability" + i).gameObject.SetActive(false);
        }
    }

    public void goFight()
    {
        PlayerPrefs.SetString("active", JsonUtility.ToJson(activeSave));
        SceneManager.LoadScene("Dungeon");
    }

    public void goBack()
    {
        PlayerPrefs.SetString("" + activeSave.saveId, JsonUtility.ToJson(activeSave));
        PlayerPrefs.SetString("active", null);
        SceneManager.LoadScene("ChooseSave");
    }
}