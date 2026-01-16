namespace LoZClone
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Xna.Framework;
    using Microsoft.Xna.Framework.Graphics;

    class BlueMoldormHead : EnemyEssentials,  IEnemy
    {
        private List<IEnemy> children;
        private Vector2 passedVelocity;
        private int timeSinceLastPass;
        private bool spawnedChildren;

        public BlueMoldormHead(Vector2 location)
        {
            RandomStateGenerator = new RandomStateGenerator(this);
            States = new Dictionary<RandomStateGenerator.StateType, int>(GameData.Instance.EnemyStateWeights.FireSnakeStateList);
            Health = new HealthManager(GameData.Instance.EnemyHealthConstants.FireSnakeHealth);
            Physics = new Physics(location);
            Physics.Mass = GameData.Instance.EnemyMassConstants.FireSnakeMass;
            Physics.IsMovable = false;
            CurrentState = new IdleEnemyState(this);
            Physics.Bounds = new Rectangle((int)Physics.Location.X, (int)Physics.Location.Y, EnemySpriteFactory.GetEnemyWidth(this), EnemySpriteFactory.GetEnemyHeight(this));
            EnemyCollisionHandler = new EnemyCollisionHandler(this);
            Expired = false;
            Damage = GameData.Instance.EnemyDamageConstants.FireSnakeDamage;
            DamageTimer = 0;
            MoveSpeed = GameData.Instance.EnemySpeedConstants.BlueFireSnakeSpeed;
            CurrentTint = LoZGame.Instance.DefaultTint;
            HasChild = true;
            children = new List<IEnemy>();
            spawnedChildren = false;
            AI = EnemyAI.MoldormHead;
            IsSpawning = false;
            DropTable = GameData.Instance.EnemyDropTables.BlueMoldormDropTable;
            ApplyDamageMod();
            ApplySmallSpeedMod();
            ApplySmallWeightModPos();
            ApplyLargeHealthMod();
            MinMaxWander = new Point(LoZGame.Instance.UpdateSpeed);
        }

        public override void TakeDamage(int damageAmount)
        {
            if (children.Count > 0 && DamageTimer <= 0)
            {
                DamageTimer = LoZGame.Instance.UpdateSpeed / 2;
                children[children.Count - 1].Expired = true;
                children.RemoveAt(children.Count - 1);
            }
            else
            {
                base.TakeDamage(damageAmount);
            }
        }

        public override void UpdateChild()
        {
            if (children.Count > 0)
            {
                for (int i = children.Count - 1; i > 0; i--)
                {
                    children[i].Physics.MovementVelocity = children[i - 1].Physics.MovementVelocity;
                }
                children[0].Physics.MovementVelocity = Physics.MovementVelocity;
            }
        }

        public override void AddChild()
        {
            if (!spawnedChildren)
            {
                for (int i = 0; i < GameData.Instance.EnemyMiscConstants.FireSnakeLength; i++)
                {
                    IEnemy child = new BlueMoldormSegment(this);
                    children.Add(child);
                    LoZGame.Instance.GameObjects.Enemies.Add(child);
                }
                spawnedChildren = true;
            }
        }

        public override void Stun(int stunTime)
        {
        }

        public override ISprite CreateCorrectSprite()
        {
            return EnemySpriteFactory.Instance.CreateFireSnakeSprite();
        }
    }
}