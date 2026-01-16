namespace Combat
{
    public class Archer : Hero , IRangedCombatCapable
    {
        public override void DoDamageToTarget(IDoDamage _target)
        {
            this.DoRangedAttack(_target);
        }

        public void DoRangedAttack(IDoDamage _target)
        {
            _target.TakeDamage(10);
        }

        public override void TakeDamage(int damage)
        {
            // Leave empty
        }


    }
}