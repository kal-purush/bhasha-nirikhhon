namespace LoZClone
{
    using System;
    using Microsoft.Xna.Framework;
    using System.Collections.Generic;

    public class Ganon : EnemyEssentials, IEnemy
    {
        private int visibilityTimer;

        public int VisibilityTimer { get { return visibilityTimer; } set { visibilityTimer = value; } }

        public Ganon(Vector2 location)
        {
            RandomStateGenerator = new RandomStateGenerator(this);
            States = new Dictionary<RandomStateGenerator.StateType, int>(GameData.Instance.EnemyStateWeights.GanonTeleportingStateList);
            Health = new HealthManager(GameData.Instance.EnemyHealthConstants.GanonHealth);
            Physics = new Physics(location);
            Physics.Mass = GameData.Instance.EnemyMassConstants.GanonMass;
            CurrentState = new IdleEnemyState(this);
            Physics.Bounds = new Rectangle((int)Physics.Location.X, (int)Physics.Location.Y, EnemySpriteFactory.GetEnemyWidth(this), EnemySpriteFactory.GetEnemyHeight(this));
            EnemyCollisionHandler = new EnemyCollisionHandler(this);
            Expired = false;
            Physics.IsMovable = false;
            visibilityTimer = 0;
            Damage = GameData.Instance.EnemyDamageConstants.GanonDamage;
            DamageTimer = 0;
            //AI = EnemyAI.Ganon;
            MoveSpeed = GameData.Instance.EnemySpeedConstants.GanonSpeed;
            CurrentTint = LoZGame.Instance.DefaultTint;
            DropTable = GameData.Instance.EnemyDropTables.EmptyDropTable;
            ApplyDamageMod();
            ApplySmallSpeedMod();
            ApplyLargeWeightModPos();
            ApplyLargeHealthMod();
        }

        public override void Stun(int stunTime)
        {
        }

        public override void Attack()
        {
            CurrentState = new AttackingGanonState(this);
        }

        public override void HandleDamage()
        {
            if (DamageTimer > 0 && Health.CurrentHealth > 4)
            {
                DamageTimer--;
                if (DamageTimer % 10 > 5)
                {
                    CurrentTint = Color.DarkSlateGray;
                }
                else
                {
                    CurrentTint = LoZGame.Instance.DefaultTint;
                }
            }
        }

        public override void TakeDamage(int damageAmount)
        {
            if (DamageTimer <= 0 && !IsSpawning)
            {
                Health.DamageHealth(damageAmount);
                if (damageAmount > 0)
                {
                    SoundFactory.Instance.PlayEnemyHit();
                    DamageTimer = LoZGame.Instance.UpdateSpeed;
                }
            }
            if (Health.CurrentHealth <= 4 && Health.CurrentHealth > 0)
            {
                DamageTimer = 0;
                visibilityTimer = 0;
                States = new Dictionary<RandomStateGenerator.StateType, int>(GameData.Instance.EnemyStateWeights.GanonParalyzedStateList);
            }
            else if (Health.CurrentHealth <= 0)
            {
                SoundFactory.Instance.PlayBossDie();
                LoZGame.Instance.Dungeon.DefeatedBoss = true;
                IsDead = true;
                CurrentState.Die();
            }
        }

        public override void Update()
        {
            HandleDamage();
            if (!LoZGame.Instance.Players[0].Inventory.HasClock || IsSpawning || IsDead)
            {
                CurrentState.Update();
                Physics.Move();
                Physics.SetDepth();
            }
            if (visibilityTimer > 0)
            {
                visibilityTimer--;
            }
            CreateCorrectSprite();
        }

        public override void OnCollisionResponse(ICollider otherCollider, CollisionDetection.CollisionSide collisionSide)
        {
            if (otherCollider is IPlayer)
            {
                EnemyCollisionHandler.OnCollisionResponse((IPlayer)otherCollider, collisionSide);
            }
            else if (otherCollider is IProjectile)
            {
                EnemyCollisionHandler.OnCollisionResponse((IProjectile)otherCollider, collisionSide);
            }
        }

        public override ISprite CreateCorrectSprite()
        {
            if (Health.CurrentHealth > 4)
            {
                if (visibilityTimer > 0)
                {
                    return EnemySpriteFactory.Instance.CreateGanonVisibleSprite();
                }
                else
                {
                    return EnemySpriteFactory.Instance.CreateGanonInvisibleSprite();
                }
            }
            else
            {
                return EnemySpriteFactory.Instance.CreateGanonParalyzedSprite();
            }
        }
    }
}