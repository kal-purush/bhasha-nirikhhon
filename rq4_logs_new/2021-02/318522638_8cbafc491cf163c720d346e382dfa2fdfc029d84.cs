using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class UIManager : MonoBehaviour
{
    [SerializeField] Canvas canvas;

    public static UIManager Instance { get; private set; }
    private MobileUIManager mobileUiManager;

    private void Awake()
    {
        if (Instance)
        {
            Debug.LogWarning("UIManager already in scene. Deleting myself!");
            Destroy(this);
            return;
        }
        Instance = this;
    }

    public void OnLocalPlayerStarted(Player player, InputType inputMethod)
    {
        if (inputMethod == InputType.Mobile)
        {
            mobileUiManager = Instantiate(InputDict.Instance.MobileUIManagerPrefab, canvas.transform);
            (player.Input as MobileInput).SetUI(mobileUiManager);
        }
        // Register to all sort of player events
    }

    public void OnLocalPlayerDeleted()
    {
        // Unregister to all sort of player events
    }

    private void OnDestroy()
    {
        if (Instance == this)
            Instance = null;
    }
}