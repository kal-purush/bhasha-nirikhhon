using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Media;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Content;

namespace SkoleProjekt_GodtNavn
{
    public class Golem : Enemy
    {
        public AnimationContainer_Sheet animationContainer_Sheet_Walk = new AnimationContainer_Sheet()
        {
            stopAtEnd = false,

            start_Up = 0,
            end_Up = 6,
            row_Up = 0,

            start_Down = 0,
            end_Down = 6,
            row_Down = 2,

            start_Left = 0,
            end_Left = 6,
            row_Left = 1,

            start_Right = 0,
            end_Right = 6,
            row_Right = 3,
        };
        public AnimationContainer_Sheet animationContainer_Sheet_Attack = new AnimationContainer_Sheet()
        {
            stopAtEnd = true,

            spriteHeightOffset = 0,
            spriteWidthOffset = 0,

            start_Up = 0,
            end_Up = 6,
            row_Up = 0,

            start_Down = 0,
            end_Down = 6,
            row_Down = 2,

            start_Left = 0,
            end_Left = 6,
            row_Left = 1,

            start_Right = 0,
            end_Right = 6,
            row_Right = 3,
        };
        public AnimationContainer_Sheet animationContainer_Sheet_Death = new AnimationContainer_Sheet()
        {
            stopAtEnd = true,

            spriteHeightOffset = 0,
            spriteWidthOffset = 0,

            start_Up = 0,
            end_Up = 6,
            row_Up = 0,

            start_Down = 0,
            end_Down = 6,
            row_Down = 0,

            start_Left = 0,
            end_Left = 6,
            row_Left = 0,

            start_Right = 0,
            end_Right = 6,
            row_Right = 0,
        };

        public Texture2D golem_Walk;
        public Texture2D golem_Attack;
        public Texture2D golem_Death;

        public DoAnimation_sheet doAnimation = new DoAnimation_sheet(5);
        public bool doDeathAnimationOneTime = true;
        public Golem()
        {
            health.maxValue = 20;
            health.currentValue = 20;
            meleeRange = 80;
            xpAmount = 200;
        }
        public Golem(int _health, Color _color, float _scale)
        {
            health.maxValue = _health;
            health.currentValue = _health;
            color = _color;
            transform.scale = _scale;
        }


        public override void Initialize()
        {
            base.Initialize();
            golem_Walk = Gameworld.spriteContainer.spriteSheet["Enemy_Golem_Walk_Sheet"];
            golem_Attack = Gameworld.spriteContainer.spriteSheet["Enemy_Golem_Attack_Sheet"];
            golem_Death = Gameworld.spriteContainer.spriteSheet["Enemy_Golem_Die_Sheet"];

            spritePositionOffset = new Vector2(0, -70);

            sprite = golem_Walk;

            doAnimation.SetAnimation(animationContainer_Sheet_Walk, facing);
            layerDepth = 0.2f;
            speed = 120f;
            transform.scale = 2f;
            bloodColor = Color.CornflowerBlue;
        }

        public override void Update(GameTime gameTime)
        {

            if (isAlive)
            {
                base.Update(gameTime);
                AnimationState();
                isAttack = doAnimation.Animate(gameTime, facing);

            }
            else if (doDeathAnimationOneTime)
            {
                AnimationState();
                isAttack = doAnimation.Animate(gameTime, facing);
                doDeathAnimationOneTime = doAnimation.Animate(gameTime, facing);
            }
            SetImagePosition();
        }

        public void SetImagePosition()
        {
            switch (facing)
            {
                case Facing.Up:
                    spritePositionOffset = new Vector2(-20, -70);
                    break;
                case Facing.Down:
                    spritePositionOffset = new Vector2(-20, -70);
                    break;
                case Facing.Left:
                    spritePositionOffset = new Vector2(-30, -70);
                    break;
                case Facing.Rigth:
                    spritePositionOffset = new Vector2(10, -70);
                    break;
                default:
                    break;
            }
        }

        public void AnimationState()
        {
            if (isAlive)
            {
                if (isAttack == true)
                {
                    sprite = golem_Attack;
                    doAnimation.SetAnimation(animationContainer_Sheet_Attack, facing);
                }
                else
                {
                    sprite = golem_Walk;
                    doAnimation.SetAnimation(animationContainer_Sheet_Walk, facing);
                }
            }
            else
            {
                sprite = golem_Death;
                doAnimation.SetAnimation(animationContainer_Sheet_Death, facing);
            }

        }

        public override Rectangle rectangle
        {
            get
            {
                return new Rectangle(

                    (sprite.Width + 0) / 7 * doAnimation.currentIndex,
                    (sprite.Height + 0) / 4 * doAnimation.currentRow,
                    sprite.Width / 7,
                    sprite.Height / 4
                    );
            }
        }
        public void NewOriginPoint()
        {
            int Xposition = (sprite.Width / 7 / 2) + doAnimation.animationContainer.spriteWidthOffset;
            int Yposition = (sprite.Height / 4 / 2) + doAnimation.animationContainer.spriteHeightOffset;
            origin = new Vector2(Xposition, Yposition);
        }

        public override Rectangle CollisionBox
        {
            get
            {
                return new Rectangle(
                    (int)transform.position.X - (int)(origin.X * transform.scale) +10,
                    (int)transform.position.Y - (int)(origin.Y * transform.scale),
                    (int)(sprite.Width / 7 * transform.scale - 60),
                    (int)(sprite.Height / 4 * transform.scale - 75)
                    );
            }
        }

        public override void Death(Player player)
        {
            base.Death(player);
            Gameworld.audioPlayer.SoundEffect_Play("golem_death", 0.1f);
        }
    }
}