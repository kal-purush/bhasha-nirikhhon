using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class ResultScript : MonoBehaviour
{
    [SerializeField] GameObject result_ui;
    [SerializeField] GameObject next_ui;

    // Start is called before the first frame update
    void Start()
    {
        
    }

    // Update is called once per frame
    void Update()
    {
        if (Input.GetKeyDown(KeyCode.Return) && !next_ui.activeSelf)
        {
            Destroy(result_ui);
            next_ui.SetActive(true);
        }
    }

    public void RePlay()
    {
        SceneManager.LoadScene("Main");
    }
}