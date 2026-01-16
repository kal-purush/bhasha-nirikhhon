namespace LoZClone
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Xna.Framework;
    using Microsoft.Xna.Framework.Graphics;

    public class MiniPatra : EnemyEssentials, IEnemy
    {
        private IEnemy parent;
        private float rotation;

        public MiniPatra(IEnemy body)
        {
            parent = body;
            Physics = new Physics(Vector2.Zero);
            rotation = -MathHelper.PiOver2;
            SetLocation();
            RandomStateGenerator = new RandomStateGenerator(this);
            States = new Dictionary<RandomStateGenerator.StateType, int>()
            {
                 { RandomStateGenerator.StateType.Idle, 1},
            };
            Health = new HealthManager(1);
            Physics.Mass = GameData.Instance.EnemyMassConstants.DragonMass;
            Physics.IsMovable = false;
            CurrentState = new IdleEnemyState(this);
            Physics.Bounds = new Rectangle((int)Physics.Location.X, (int)Physics.Location.Y, EnemySpriteFactory.GetEnemyWidth(this), EnemySpriteFactory.GetEnemyHeight(this));
            EnemyCollisionHandler = new EnemyCollisionHandler(this);
            Expired = false;
            Damage = GameData.Instance.EnemyDamageConstants.FullHeart + GameData.Instance.EnemyDamageConstants.HalfHeart;
            MoveSpeed = GameData.Instance.EnemySpeedConstants.GleeokHeadSpeed;
            DamageTimer = 0;
            MinMaxWander = new Point(LoZGame.Instance.UpdateSpeed / 4, LoZGame.Instance.UpdateSpeed);
            CurrentTint = LoZGame.Instance.DefaultTint;
            HasChild = false;
            AI = EnemyAI.MiniPatra;
            IsSpawning = false;
            DropTable = GameData.Instance.EnemyDropTables.EmptyDropTable;
            ApplyDamageMod();
            ApplySmallWeightModPos();
        }

        public override void Stun(int stunTime)
        {
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

        public override void OnCollisionResponse(int sourceWidth, int sourceHeight, CollisionDetection.CollisionSide collisionSide)
        {
            // ignore obstacles
        }

        private void SetLocation()
        {
            // very bad way to pass rotation from parent to child, our code doesnt yet support thiss and I plan on moving circular motion around a point to Phyics at somepoint
            if (MoveSpeed > MathHelper.TwoPi)
            {
                MoveSpeed -= MathHelper.TwoPi;
            }
            rotation = MoveSpeed;
            int x = parent.Physics.Bounds.Center.X + (int)((float)GameData.Instance.EnemyMiscConstants.NormalMiniPatraOffset * Math.Cos(rotation));
            int y = parent.Physics.Bounds.Center.Y + (int)((float)GameData.Instance.EnemyMiscConstants.NormalMiniPatraOffset * Math.Sin(rotation));
            Point newLoc = new Point(x - (Physics.Bounds.Width / 2), y - (Physics.Bounds.Height / 2));
            Physics.Bounds = new Rectangle(newLoc, Physics.Bounds.Size);
            Physics.SetLocation();
            Physics.SetDepth();
        }

        public override void Update()
        {
            HandleDamage();
            if (!LoZGame.Instance.Players[0].Inventory.HasClock || IsSpawning || IsDead)
            {
                CurrentState.Update();
                SetLocation();
            }
        }

        public override ISprite CreateCorrectSprite()
        {
            return EnemySpriteFactory.Instance.CreateMiniPatraSprite();
        }
    }
}