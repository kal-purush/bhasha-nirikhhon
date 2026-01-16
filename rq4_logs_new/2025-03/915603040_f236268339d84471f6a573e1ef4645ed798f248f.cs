using UnityEngine;

[CreateAssetMenu(fileName = "DamageBoostItem", menuName = "Items/DamageBoostItem")]
public class DamageBoostItemSO : CharacterStatModifierSO
{
    [SerializeField]
    private float duration = 10f; 

    [SerializeField]
    private int damageIncrease = 10;

    [SerializeField] 
    private BuffDataSO buffData;

    public override void AffectChararacter(GameObject character, float val)
    {
        PlayerController player = character.GetComponent<PlayerController>();
        if (player != null)
        {
            // Lấy tất cả các Attack components (bao gồm SwordAttack2, AirAttack_1, AirAttack_2, v.v.)
            Attack[] attacks = character.GetComponentsInChildren<Attack>();
            if (attacks != null && attacks.Length > 0)
            {
                player.StartCoroutine(ApplyDamageBoost(attacks));
                BuffUIManager.Instance.AddBuffUI(buffData, duration);
            }
            else
            {
                Debug.LogWarning("No Attack components found on the player or its children.");
            }
        }
    }

    private System.Collections.IEnumerator ApplyDamageBoost(Attack[] attacks)
    {
        foreach (Attack attack in attacks)
        {
            attack.baseAttackDamage += damageIncrease;
            Debug.Log($"{attack.gameObject.name}'s damage boosted by {damageIncrease}. New damage: {attack.baseAttackDamage}");
        }

        yield return new WaitForSeconds(duration);

        foreach (Attack attack in attacks)
        {
            attack.baseAttackDamage -= damageIncrease;
            Debug.Log($"{attack.gameObject.name}'s damage boost expired. New damage: {attack.baseAttackDamage}");
        }
    }
}