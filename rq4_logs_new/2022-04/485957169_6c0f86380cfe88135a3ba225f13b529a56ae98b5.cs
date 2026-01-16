using NUnit.Framework;
using Roleplay;
using System.Collections.Generic;

namespace Test.Library
{


    public class WizardTest
    {
        private Wizard wizard;

        private Spell spell;

        private List<Spell> spellList;

        private SpellBook spellBook;

        private Cape cape;
        [SetUp]
        public void SetUp()
        {
            this.spell = new Spell("Fireball", 30);
            this.spellList = new List<Spell>();
            this.spellBook = new SpellBook(this.spellList);
            this.cape = new Cape(0,10);
            this.wizard = new Wizard("Merlín", this.spellBook);
            this.spellBook.AddSpell(this.spell);
            this.spellBook.CastSpell(this.spell);
        }

        //Test para saber el ataque de un mago.
        [Test]
        public void GetAttackTest()
        {
            int expectedAttack = 45;
            Assert.AreEqual(expectedAttack,this.wizard.GetAttack());
        }
        
        //Test para verificar que un mago 
        [Test]
        public void AttackWarriorWithWizardTest()
        {
            
        }
    }
}