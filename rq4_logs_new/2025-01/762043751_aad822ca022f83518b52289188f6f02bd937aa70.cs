using Game.Ability;

namespace Game.UI.Windows
{
    public class AbilityDefinitionInspectable : IAbilityInspectable
    {
        private AbilityDefinition abilityDefinition;

        public AbilityDefinitionInspectable(AbilityDefinition abilityDefinition)
        {
            this.abilityDefinition = abilityDefinition;
        }

        public float GetCooldown()
        {
            return abilityDefinition.TotalCooldown;
        }

        public string GetDescription()
        {
            return abilityDefinition.ParseDescription(null);
        }

        public string GetTitle()
        {
            return abilityDefinition.Title;
        }
    }
}