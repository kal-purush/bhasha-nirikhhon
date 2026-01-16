using UnityEngine;

public class CharacterStats : MonoBehaviour
{
    [Header("Character Settings")]
    [SerializeField] public CharacterClass CharacterClass;
    // Hp in float cuz int isn't smooth drain
    [SerializeField] public float maxHealth = 100;
    // Hp drain without oxygen
    [SerializeField] public float healthDrainNoOxygen = 5f;

    [Header("Oxygen Settings")]
    [SerializeField] public float oxygenMax = 100;
    // Oxygen drain rates
    [SerializeField] public float oxygenDrainAlways = 1f;
    [SerializeField] public float oxygenDrainSprint = 2f;
    [SerializeField] public float oxygenDrainJump = 1f;
    [SerializeField] public float oxygenDrainSwim = 10f;
    [SerializeField] public float oxygenDrainCarryBase = 1f;

    void Start()
    {
        ApplyClassModifiers();
    }

    // Apply class specific modifiers
    private void ApplyClassModifiers()
    {
        if (CharacterClass == CharacterClass.Strongman)
        {
            oxygenDrainCarryBase = 0.5f;
        }
    }
}