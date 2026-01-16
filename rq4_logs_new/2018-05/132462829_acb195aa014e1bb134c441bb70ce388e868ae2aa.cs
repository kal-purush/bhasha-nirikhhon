using System.Collections;
using System.Collections.Generic;
using UnityEditor.SceneManagement;
using UnityEngine;
using UnityEngine.UI;

public class Button_LoadSence : MonoBehaviour {
    public Button find;
    public Button create;
    public Button back;

    // Use this for initialization
    void Start () {
        if (create != null)
        {
            create.onClick.AddListener(delegate ()
            {
                Invoke("Jump", 0.5F);
                EditorSceneManager.LoadScene("Scene/Main");
            });
        }

        if (find != null)
        {
            find.onClick.AddListener(delegate ()
            {
                Invoke("Jump", 0.5F);
                EditorSceneManager.LoadScene("Scene/Roommenu");
            });
        }

        if (back != null)
        {
            back.onClick.AddListener(delegate ()
            {
                Invoke("Jump", 0.5F);
                EditorSceneManager.LoadScene("Scene/Startmenu");
            });
        }
    }
	
	// Update is called once per frame
	void Update () {
		
	}
}