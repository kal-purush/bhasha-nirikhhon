using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Engine
{
    class MagicWeapon : Item
    {

        /// <summary>
        /// Esta clase debería tener no sólo el constructor, sino un generador de nombres y un generador de bonus
        /// Un número aleatorio para decidir el tipo de arma
        /// Un número aleatorio por cada bonus para decidir los valores
        /// dependiendo de los valores un mecanismo para asignarle un nombre especial.
        /// </summary>
        
        public int MinimumDamage { get; set; }
        public int MaximumDamage { get; set; }
        public int BonusToStrength { get; set; }
        public int BonusToDexterity { get; set; }
        public int BonusToLife { get; set; }
        public string[] WeaponType = { "Espada", "Hacha", "Daga", "Lanza", "Maza" };
        public string[] AdjetivoFuerza = { null, "Recia", "Firme", "Poderosa" };
        public string[] AdjetivoDestreza = { null, "Sutíl", "Certera", "Precisa" };
        public string[] AdjetivoVida = { null, "Encantada", "Bendita", "Sagrada" };

        public MagicWeapon ( int id, string name, string specialWeaponNames, string namePlural, int minimumDamage, int maximumDamage, int bonusToStrength, int bonusToDexterity, int bonusToLife) : base (id, name, namePlural)
        {
            MinimumDamage = minimumDamage;
            MaximumDamage = maximumDamage;
            BonusToStrength = bonusToStrength;
            BonusToDexterity = bonusToDexterity;
            BonusToLife = bonusToLife;
            
        }

        /*public Weapon CreateNewMagicWeapon()
        {
            int id;
            int minimumDamage;
            int maximumDamage;
            int bonusToStrength;
            int bonusToDexterity;
            int bonusToLife;
            string tempWeaponName;

            // Primer número aleatorio para decidir el tipo de arma
            int tipo = RandomNumberGenerator.NumberBetween(0, 4);

            int bonus = RandomNumberGenerator.NumberBetween(1, 3);

            switch (bonus)
            {
                case 1: bonusToStrength = RandomNumberGenerator.NumberBetween(1, 3);
                    break;
                case 2: bonusToDexterity = RandomNumberGenerator.NumberBetween(1, 3);
                    break;
                case 3: bonusToLife = RandomNumberGenerator.NumberBetween(1, 3);
                    break;
                default: BonusToLife = 1;
                    break;
            }

            if(BonusToStrength > 0)
            {
                tempWeaponName = AdjetivoFuerza[bonusToStrength];
            } else if (BonusToDexterity > 0)
            {
                tempWeaponName = AdjetivoDestreza[bonusToDexterity];
            } else
            {
                tempWeaponName = AdjetivoVida[bonusToLife];
            }

            return new MagicWeapon(id, WeaponType[tipo], tempWeaponName, null, 1, 4, bonusToStrength, bonusToDexterity, bonusToLife);

        }
    }*/
}