using UnityEngine;
using UnityEngine.UI;

public class ButtonPressHandler : MonoBehaviour
{
    public GameObject A;
    public GameObject B;
    public GameObject C;
    public GameObject D;
    public Button B1;
    public Button B2;

    private bool B1PressedBefore = false;
    private bool B2PressedBefore = false;

    void Start()
    {
        // Set the initial active states
        A.SetActive(true);
        B.SetActive(false);
        C.SetActive(false);
        D.SetActive(false);

        // Add listeners for the buttons
        B1.onClick.AddListener(OnB1Pressed);
        B2.onClick.AddListener(OnB2Pressed);
    }

    void OnB1Pressed()
    {
        // Deactivate all GameObjects
        DeactivateAll();

        if (B2PressedBefore)
        {
            D.SetActive(true);
        }
        else
        {
            B.SetActive(true);
        }

        B1PressedBefore = true;
    }

    void OnB2Pressed()
    {
        // Deactivate all GameObjects
        DeactivateAll();

        if (B1PressedBefore)
        {
            D.SetActive(true);
        }
        else
        {
            C.SetActive(true);
        }

        B2PressedBefore = true;
    }

    void DeactivateAll()
    {
        A.SetActive(false);
        B.SetActive(false);
        C.SetActive(false);
        D.SetActive(false);
    }
}