using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

public class ArcadePanel : MonoBehaviour
{
    public Dropdown presetDropdown;
    public Dropdown wordDifficultyDropdown;
    public Dropdown maxQuestWordsDropdown;
    public Dropdown numPuzzleLettersDropdown;

    private Animator anim;

    private List<GameManager.LevelSettings> arcadePresets = new List<GameManager.LevelSettings>();

    private void Awake()
    {
        anim = GetComponent<Animator>();
    }

    // Start is called before the first frame update
    void Start()
    {
        arcadePresets.Add(new GameManager.LevelSettings("Easy", GameManager.DifficultyLevel.Easy, 5, 1));
        arcadePresets.Add(new GameManager.LevelSettings("Normal", GameManager.DifficultyLevel.Normal, 7, 1));
        arcadePresets.Add(new GameManager.LevelSettings("Hard", GameManager.DifficultyLevel.Hard, 7, 3));
        arcadePresets.Add(new GameManager.LevelSettings("Expert", GameManager.DifficultyLevel.Expert, 7, 7));
        arcadePresets.Add(new GameManager.LevelSettings("Custom", GameManager.DifficultyLevel.Expert, 7, 7));

        presetDropdown.ClearOptions();
        presetDropdown.AddOptions(arcadePresets.Select(x => x.Name).ToList());

        if(GameManager.Instance.ArcadeSettings != null)
        {
            wordDifficultyDropdown.SetValueWithoutNotify((int)GameManager.Instance.ArcadeSettings.WordDifficulty);
            numPuzzleLettersDropdown.SetValueWithoutNotify(GameManager.Instance.ArcadeSettings.NumPuzzleLetters - 5);
            maxQuestWordsDropdown.SetValueWithoutNotify(GameManager.Instance.ArcadeSettings.MaxQuestWords - 1);
        }

    }

    public void SetPreset(int value)
    {
        wordDifficultyDropdown.SetValueWithoutNotify((int)arcadePresets[value].WordDifficulty); // assumes difficulty dropdown in same order with same names
        maxQuestWordsDropdown.SetValueWithoutNotify(arcadePresets[value].MaxQuestWords-1); // assumes dropdown goes from 1 to 7
        numPuzzleLettersDropdown.SetValueWithoutNotify(arcadePresets[value].NumPuzzleLetters - 5); // assumes dropdown goes from 5 to 7
    }

    public void SetParamDropdown(int value)
    {
        // called for all other dropdowns
        presetDropdown.SetValueWithoutNotify(4);
    }

    
    public void StartButton() // start arcade mode
    {
        anim.SetTrigger("Start");
        GameManager.Instance.ResetPlayerLevel();
        GameManager.Instance.Mode = GameManager.GameMode.Arcade;
        GameManager.Instance.ArcadeSettings = new GameManager.LevelSettings(
            "Custom",
            (GameManager.DifficultyLevel)wordDifficultyDropdown.value,
            numPuzzleLettersDropdown.value + 5,
            maxQuestWordsDropdown.value + 1);
        GameManager.Instance.SavePlayerData();

        StartCoroutine(StartButtonCR());
    }

    private IEnumerator StartButtonCR()
    {
        while (true)
            if (anim.GetCurrentAnimatorStateInfo(0).IsName("ArcadePanelIdle"))
                break;
            else
            {
                yield return null;
            }

        SceneManager.LoadScene(SceneManager.GetActiveScene().buildIndex + 1);
        yield return null;
    }
}