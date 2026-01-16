using UnityEngine;
using UnityEngine.UI;
using TPS.Armor;

namespace TPS.UI
{
    public class ArmorBar : MonoBehaviour
    {
        [SerializeField] private GameObject entityToWatch;
        [SerializeField] private Slider slider;
        [SerializeField] private Image armorIcon;
        [SerializeField] private Sprite nullArmorSprite;

        private CharacterArmorController armorController;

        private void Awake()
        {
            if (entityToWatch == null)
            {
                Debug.LogError("ArmorBar: entityToWatch is not set!");
                return;
            }

            if (!entityToWatch.TryGetComponent(out armorController))
            {
                Debug.LogError("ArmorBar: entityToWatch does not have CharacterArmorController component!");
                return;
            }

            armorController.OnArmorDurabilityChange.AddListener(UpdateArmorBar);
            armorController.OnArmorChange.AddListener(UpdateArmor);
        }

        private void OnDestroy()
        {
            armorController.OnArmorDurabilityChange.RemoveListener(UpdateArmorBar);
            armorController.OnArmorChange.RemoveListener(UpdateArmor);
        }

        private void UpdateArmorBar(ArmorItem armorItem)
        {
            slider.value = armorItem == null ? 0 : armorItem.currentDurability / armorItem.armor.baseDurability * 100;
        }

        private void UpdateArmor(ArmorItem armorItem)
        {
            armorIcon.sprite = armorItem != null ? armorItem.armor.icon : nullArmorSprite;
            UpdateArmorBar(armorItem);
        }
    }
}