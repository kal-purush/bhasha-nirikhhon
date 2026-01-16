using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;

namespace SkoleProjekt_GodtNavn
{
    public class Goblin : Enemy
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

            start_Up = 6,
            end_Up = 10,
            row_Up = 0,

            start_Down = 6,
            end_Down = 10,
            row_Down = 2,

            start_Left = 6,
            end_Left = 10,
            row_Left = 1,

            start_Right = 6,
            end_Right = 10,
            row_Right = 3,
        };

        public DoAnimation_sheet doAnimation = new DoAnimation_sheet(5);
        public override void Initialize()
        {
            base.Initialize();
            sprite = Gameworld.spriteContainer.spriteSheet["Enemy_Goblin_Sheet"];
            doAnimation.SetAnimation(animationContainer_Sheet_Walk, facing);
            layerDepth = 0.2f;
            speed = 120f;
            transform.scale = 2f;
        }

        public override void Update(GameTime gameTime)
        {
            base.Update(gameTime); 
            AttackCheck();
            isAttack = doAnimation.Animate(gameTime, facing);
        }

        public void AttackCheck()
        {
            if(isAttack == true)
            {
                doAnimation.SetAnimation(animationContainer_Sheet_Attack, facing);
            }
            else
            {
                doAnimation.SetAnimation(animationContainer_Sheet_Walk, facing);
            }
        }

        public override Rectangle rectangle
        {
            get
            {
                return new Rectangle(

                    (sprite.Width + 20) / 11 * doAnimation.currentIndex,
                    (sprite.Height + 2) / 5 * doAnimation.currentRow,
                    sprite.Width / 11,
                    sprite.Height / 5
                    );
            }
        }
        public void NewOriginPoint()
        {
            int Xposition = (sprite.Width / 11 / 2) + doAnimation.animationContainer.spriteWidthOffset;
            int Yposition = (sprite.Height / 5 / 2) + doAnimation.animationContainer.spriteHeightOffset;
            origin = new Vector2(Xposition, Yposition);
        }

        public override Rectangle CollisionBox
        {
            get
            {
                return new Rectangle(
                    (int)transform.position.X - (int)(origin.X * transform.scale),
                    (int)transform.position.Y - (int)(origin.Y * transform.scale),
                    (int)(sprite.Width / 11 * transform.scale),
                    (int)(sprite.Height / 5 * transform.scale)
                    );
            }
        }
    }
}