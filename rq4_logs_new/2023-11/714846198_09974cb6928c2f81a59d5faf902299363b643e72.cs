namespace TeamB_TD
{
    namespace Battle
    {
        namespace Unit
        {
            namespace Ally
            {
                public class BombAttack : IAllyAttack
                {
                    public float AttackAnimationTime => 0.1f;

                    public void Fire(float attackPower)
                    {

                    }

                    public bool IsAnyObjectInTrigger()
                    {
                        return true;
                    }
                }
            }
        }
    }
}