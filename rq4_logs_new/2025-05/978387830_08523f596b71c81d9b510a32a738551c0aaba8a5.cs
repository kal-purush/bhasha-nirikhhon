using UnityEngine;
using TMPro; // or TMPro if using TextMeshPro

public class AttackSelectorUI : MonoBehaviour
{
    public TMP_Text selectedAttackText; // Drag your text component here
    private string selectedAttack = "1 (A)";

    void Update()
    {
        // Check number keys
        if (Input.GetKeyDown(KeyCode.Alpha1)) { selectedAttack = "1 (A)"; Debug.Log("1");}
        if (Input.GetKeyDown(KeyCode.Alpha2)) { selectedAttack = "2 (B)"; Debug.Log("2");}
        if (Input.GetKeyDown(KeyCode.Alpha3)) { selectedAttack = "3 (C)"; Debug.Log("3");}
        if (Input.GetKeyDown(KeyCode.Alpha4)) { selectedAttack = "4 (D)"; Debug.Log("4");}
        if (Input.GetKeyDown(KeyCode.Alpha5)) { selectedAttack = "5 (E)"; Debug.Log("4");}
        if (Input.GetKeyDown(KeyCode.Alpha6)) { selectedAttack = "6 (F)"; Debug.Log("4");}
        if (Input.GetKeyDown(KeyCode.Alpha7)) { selectedAttack = "7 (G)"; Debug.Log("4");}

        // Update the text
        selectedAttackText.text = "Selected Note: " + selectedAttack;
    }
}