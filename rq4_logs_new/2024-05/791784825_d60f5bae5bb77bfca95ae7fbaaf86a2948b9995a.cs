using UnityEditor;
using UnityEngine;
using UnityEngine.SceneManagement;

public class Gate : MonoBehaviour
{
    public int requiredEnemiesDefeated = 10; // Adjust this value based on your game's design
    public SceneAsset nextScene; // Use SceneAsset type for easy scene assignment
    public Animator gateAnimator; // Reference to the gate animator
    private int enemiesDefeated = 0;
    private bool gateActivated = false;

    private void OnTriggerEnter(Collider other)
    {
        if (other.CompareTag("Player") && gateActivated)
        {
            // Load the next scene
            SceneManager.LoadScene(nextScene.name);
        }
    }

    // Call this method when an enemy is defeated
    public void EnemyDefeated()
    {
        enemiesDefeated++;
        Debug.Log("Enemies Defeated: " + enemiesDefeated);

        // If the required number of enemies are defeated, activate the gate
        if (enemiesDefeated >= requiredEnemiesDefeated && !gateActivated)
        {
            ActivateGate();
        }
    }

    private void ActivateGate()
    {
        // Play gate animation
        gateAnimator.SetTrigger("Open");

        // Perform gate activation logic here
        Debug.Log("Gate activated!");
        gateActivated = true;
    }
}