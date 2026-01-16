namespace PersonalDevelopment
{
    public class BotMeleeBehaviour : Enemy
    {
        private BotMovementBehaviour _botMovement = null;
        private BotMeleeAttackBehaviour _botMelee = null;
        
        protected override void Patrol()
        {
            _botMovement.MovementUpdate();
        }

        protected override void Attack()
        {
            _state = EnemyState.Patrol;
        }

        protected override void Awake()
        {
            base.Awake();
            _botMovement = GetComponent<BotMovementBehaviour>();
            _botMelee = GetComponent<BotMeleeAttackBehaviour>();
        }

        private void Start()
        {
            _botMovement.Initialize(_rigidBody, _properties);
            _botMelee.Initialize(_properties);
        }
    }
}