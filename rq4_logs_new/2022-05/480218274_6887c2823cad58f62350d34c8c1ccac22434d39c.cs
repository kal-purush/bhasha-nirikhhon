using System.Collections;
using System.Collections.Generic;
using UnityEngine.SceneManagement;
using UnityEngine;

public class ControlUI : MonoBehaviour
{
    // Start is called before the first frame update
    private Controls _controls;
    public GameObject ControlMenu;
    public bool isPaused;

    private void Awake()
    {
        _controls = new Controls();
        Cursor.lockState = CursorLockMode.None;
        Time.timeScale = 0f;
    }

    private void OnEnable()
    {
        _controls.Enable();
    }

    private void OnDisable()
    {
        _controls.Disable();
    }

    public void CloseControl(string menuScene)
    {
        Time.timeScale = 1f;
        ControlMenu.SetActive(false);
        Cursor.lockState = CursorLockMode.Locked;
    }

    public void Update()
    {

    }
}