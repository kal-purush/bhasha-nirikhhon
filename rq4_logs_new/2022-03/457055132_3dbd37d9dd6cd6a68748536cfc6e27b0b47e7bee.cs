using System.Collections.Generic;

namespace Assigment4
{
    public class BaseSwordData : ISwordData
    {
        private readonly int _damage;

        public BaseSwordData(int basePhysicDamage)
        {
            _damage = basePhysicDamage;
        }

        public int GetTotalDamage()
        {
            return _damage;
        }
        
        public Dictionary<Elements, int> GetElementsDamage()
        {
            return new Dictionary<Elements, int>()
            {
                { Elements.Physic, _damage }
            };
        }
        
        public string GetTextDamage()
        {
            return $"<b><color=#808080>Physic</color> damage = <color=#808080>{_damage}</color></b>";
        }
    }
}