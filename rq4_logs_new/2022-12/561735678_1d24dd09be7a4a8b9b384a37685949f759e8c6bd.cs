using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class ChooseSaveUI : MonoBehaviour
{
    public GameObject grid;

    private Save save0;
    private Save save1;
    private Save save2;
    // Start is called before the first frame update
    void Start()
    {
        save0 = JsonUtility.FromJson<Save>(PlayerPrefs.GetString("" + 0));
        if (save0 != null)
        {
            var saveText = grid.transform.Find("Save" + 0).transform.Find("Text").gameObject;
            saveText.GetComponent<Text>().text = save0.playerName + ", " + save0.playerCurrentLevel;
        }

        save1 = JsonUtility.FromJson<Save>(PlayerPrefs.GetString("" + 1));
        if (save1 != null)
        {
            var saveText = grid.transform.Find("Save" + 1).transform.Find("Text").gameObject;
            saveText.GetComponent<Text>().text = save1.playerName + ", " + save1.playerCurrentLevel;
        }

        save2 = JsonUtility.FromJson<Save>(PlayerPrefs.GetString("" + 2));
        if (save0 != null)
        {
            var saveText = grid.transform.Find("Save" + 2).transform.Find("Text").gameObject;
            saveText.GetComponent<Text>().text = save2.playerName + ", " + save2.playerCurrentLevel;
        }
    }

    public void ChooseSave(int id)
    {
        PlayerPrefs.SetInt("active", id);
        SceneManager.LoadScene("MetaGame");
    }
}