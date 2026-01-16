using UnityEngine;

namespace GB_Platformer
{
    internal sealed class EnemyAttack : IExecute
    {
        private readonly EnemyInfo _enemyInfo;
        private readonly SpriteAnimator _spriteAnimator;
        private float _timer;

        public EnemyAttack(EnemyInfo enemyInfo, SpriteAnimator spriteAnimator)
        {
            _enemyInfo = enemyInfo;
            _spriteAnimator = spriteAnimator;
        }

        public void Execute()
        {
            if (_enemyInfo.IsDeath)
            {
                return;
            }
            if (!_enemyInfo.InAttackDistance)
            {
                _timer = Constants.Variables.DELAY_ATTACK;
                return;
            }

            if (_timer > 0)
            {
                _timer -= Time.deltaTime;
            }
            else
            {
                _timer = Constants.Variables.DELAY_ATTACK;
                Attack(CheckEnemyTrack(_enemyInfo.EnemyType), _enemyInfo.DamageAttack);
            }
        }

        private void Attack(Track track, float damage)
        {
            _spriteAnimator.StartAnimation(_enemyInfo.EnemyView.SpriteRenderer, track, false, Constants.Variables.ANIMATIONS_SPEED);

            Collider2D[] hitPlayers = new Collider2D[3];
            Physics2D.OverlapCircleNonAlloc(_enemyInfo.EnemyView.AttackPointTransform.position,
                _enemyInfo.EnemyView.AttackRange, hitPlayers, LayerMask.GetMask(Constants.Layer.PLAYER));

            foreach (Collider2D collider2D in hitPlayers)
            {
                if (collider2D != null && collider2D.TryGetComponent(out PlayerView player))
                {
                    player.TakeDamage(damage);
                }
            }
        }

        private Track CheckEnemyTrack(EnemyType enemyType)
        {
            return enemyType switch
            {
                EnemyType.Patrol => Track.Skeleton_Attack1,
                EnemyType.FlyPatrol => Track.FlyingEye_Attack2,
                _ => Track.None,
            };
        }
    }
}