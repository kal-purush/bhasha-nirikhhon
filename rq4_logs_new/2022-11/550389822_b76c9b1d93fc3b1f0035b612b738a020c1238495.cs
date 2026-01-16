using System;

namespace Inheritance
{
  abstract class Character
  {
    private string characterType;
    protected Character(string characterType)
    {
      this.characterType = characterType;
    }

    public abstract int DamagePoints(Character target);
    public virtual bool Vulnerable() => false;
    public override string ToString() => $"Character is a {this.characterType}";
  }

  class Warrior : Character
  {
    public Warrior() : base("Warrior") { }

    public override int DamagePoints(Character target)
    {
      return target.Vulnerable() ? 10 : 6;
    }
  }

  class Wizard : Character
  {
    private bool prepared;
    public Wizard() : base("Wizard")
    {
      this.prepared = false;
    }

    public override bool Vulnerable() => !prepared;
    public override int DamagePoints(Character target)
    {
      int damage = prepared ? 12 : 3;
      prepared = false;
      return damage;
    }
    public void PrepareSpell() => this.prepared = true;
  }

}
