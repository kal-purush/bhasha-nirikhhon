using NUnit.Framework;
using Roleplay;

namespace Test.Library
{
    public class SpellBookTest
    {
        private Spell spell;
        private SpellBook spellBook;

        [SetUp]
        public void SetUp()
        {
            this.spell = new Spell("Fireball", 30);
            this.spellBook = new SpellBook();
        }

        [Test]
        public void NullSpellTest()
        {
            Assert.IsNull(this.spellBook.spell);
        }

        [Test]
        public void AddSpellTest()
        {
            this.spellBook.AddSpell(this.spell);
            Assert.IsNotNull(this.spellBook);
        }
    }
}