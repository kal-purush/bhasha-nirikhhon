using UnityEngine;

[CreateAssetMenu(fileName = "DamageReductionItem", menuName = "Items/DamageReductionItem")]
public class CharacterStatDamageReductionModifierSO : CharacterStatModifierSO
{
    [SerializeField]
    private float duration = 10f; // Thời gian hiệu lực của buff

    [SerializeField]
    private float damageReductionPercentage = 50f; // Phần trăm giảm sát thương (0-100%)

    [SerializeField]
    private BuffDataSO buffData; // Tham chiếu đến BuffDataSO để hiển thị UI

    public override void AffectChararacter(GameObject character, float val)
    {
        PlayerController player = character.GetComponent<PlayerController>();
        if (player != null && player.IsALive) // Kiểm tra nếu nhân vật còn sống
        {
            Damageable damageable = character.GetComponent<Damageable>();
            if (damageable != null)
            {
                // Lưu trạng thái ban đầu để khôi phục sau khi buff hết
                float originalReduction = damageable.damageReductionPercentage;
                damageable.damageReductionPercentage = damageReductionPercentage;

                // Giao việc cho PlayerController quản lý coroutine
                player.StartCoroutine(ApplyDamageReduction(damageable, originalReduction));
                // Hiển thị UI buff
                BuffUIManager.Instance.AddBuffUI(buffData, duration);
            }
            else
            {
                Debug.LogWarning("Damageable component not found on the character!");
            }
        }
        else
        {
            Debug.LogWarning("PlayerController not found or character is not alive!");
        }
    }

    private System.Collections.IEnumerator ApplyDamageReduction(Damageable damageable, float originalReduction)
    {
        yield return new WaitForSeconds(duration);

        if (damageable != null && damageable.IsAlive)
        {
            // Khôi phục phần trăm giảm sát thương ban đầu
            damageable.damageReductionPercentage = originalReduction;
            Debug.Log("Damage reduction buff expired. Restored original reduction.");
        }
    }
}