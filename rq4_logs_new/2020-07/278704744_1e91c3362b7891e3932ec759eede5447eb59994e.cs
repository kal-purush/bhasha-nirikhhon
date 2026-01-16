using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class MenuManager : MonoBehaviour
{
    [SerializeField] private GameObject pausePanel;
    [SerializeField] private GameObject howToPlayPanel;

    bool howToPlayActive;
    bool pauseMenuActive;

    // Start is called before the first frame update
    void Start()
    {
        howToPlayActive = true;
        ToggleHowToPlay();

        TogglePauseMenu(false);
    }

    // Update is called once per frame
    void Update()
    {

    }

    public void ToggleHowToPlay()
    {
        howToPlayActive = !howToPlayActive;
        howToPlayPanel.SetActive(howToPlayActive);
    }

    public void TogglePauseMenu(bool pauseMenuActive)
    {
        pausePanel.SetActive(pauseMenuActive);
    }
}