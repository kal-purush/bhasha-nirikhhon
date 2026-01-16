using System;
using GTA;
using GTA.Math;
using ZumbisModV.Interfaces;

namespace ZumbisModV
{
    [Serializable]
    public class BuildableInventoryItem : InventoryItemBase, IProp, ICraftable
    {
        public string PropName { get; set; }
        public BlipSprite BlipSprite { get; set; }
        public BlipColor BlipColor { get; set; }
        public Vector3 GroundOffset { get; set; }
        public bool Interactable { get; set; }
        public bool IsDoor { get; set; }
        public bool CanBePickedUp { get; set; }
        public CraftableItemComponent[] RequiredComponents { get; set; }

        /// <summary>
        /// Inicializa uma nova instância da classe <see cref="BuildableInventoryItem"/>.
        /// </summary>
        /// <param name="amount">A quantidade inicial do item.</param>
        /// <param name="maxAmount">A quantidade máxima permitida do item.</param>
        /// <param name="id">O identificador do item.</param>
        /// <param name="description">A descrição do item.</param>
        /// <param name="propName">O nome do objeto associado ao item.</param>
        /// <param name="blipSprite">O ícone de mapa utilizado para o item.</param>
        /// <param name="blipColor">A cor do ícone de mapa utilizado para o item.</param>
        /// <param name="groundOffset">A altura do item em relação ao chão.</param>
        /// <param name="interactable">Indica se o item pode ser interagido.</param>
        /// <param name="isDoor">Indica se o item é uma porta.</param>
        /// <param name="canBePickedUp">Indica se o item pode ser coletado.</param>
        public BuildableInventoryItem(
            int amount,
            int maxAmount,
            string id,
            string description,
            string propName,
            BlipSprite blipSprite,
            BlipColor blipColor,
            Vector3 groundOffset,
            bool interactable,
            bool isDoor,
            bool canBePickedUp
        )
            : base(amount, maxAmount, id, description)
        {
            PropName = propName;
            BlipSprite = blipSprite;
            BlipColor = blipColor;
            GroundOffset = groundOffset;
            Interactable = interactable;
            IsDoor = isDoor;
            CanBePickedUp = canBePickedUp;
        }
    }
}