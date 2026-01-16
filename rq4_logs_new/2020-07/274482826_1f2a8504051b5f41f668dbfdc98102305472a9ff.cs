using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using System;

namespace PixelHunter1995.Components.Alpha
{
    static class Utils
    {
        public static TComponent GetComponent<TComponent>(this IHasComponentAlpha<TComponent> owner)
            where TComponent : IComponentAlpha
        {
            return owner.Component;
        }

        /// <summary>
        /// Doesn't work, the fact it is called through reflection makes it a runtime error...
        /// </summary>
        /// <typeparam name="TComponent"></typeparam>
        /// <param name="owner"></param>
        //public static void AddMixins<TComponent>(IHasComponentAlpha<TComponent> owner)
        //    where TComponent : IComponent
        //{
        public static void AddMixins(IHasComponentsAlpha owner)
        {
                foreach (Type tinterface in (owner.GetType().GetInterfaces()))
            {
                if (tinterface.IsGenericType && tinterface.GetGenericTypeDefinition() == typeof(IHasComponentAlpha<>))
                {
                    var component = Activator.CreateInstance(tinterface.GenericTypeArguments[0]);

                    tinterface.GetProperty("Component").SetValue(owner, component);

                    var method = tinterface.GenericTypeArguments[0].GetMethod("SetOwner");
                    if (method != null)
                    {
                        method.MakeGenericMethod(owner.GetType()).Invoke(component, new object[] { owner });
                    }
                }
            }
        }
    }

    class PlayerAlpha : IPlayer, IUpdateable, IDrawable, IHasComponentAlpha<PositionComponentAlpha>, IHasComponentAlpha<SpriteComponentAlpha>, IHasComponentsAlpha
    {
        Vector2 MovePosition { get; set; }

        //PositionComponentAlpha IHasComponentGamma<PositionComponentAlpha>.Component => new PositionComponentAlpha();
        //SpriteComponentAlpha IHasComponentGamma<SpriteComponentAlpha>.Component => new SpriteComponentAlpha();
        /*
        private PositionComponentAlpha PosComp { get; set; }
        private SpriteComponentAlpha SpriteComp { get; set; }
        PositionComponentAlpha IHasComponentGamma<PositionComponentAlpha>.Component { get => this.PosComp; set => this.PosComp = value; }
        SpriteComponentAlpha IHasComponentGamma<SpriteComponentAlpha>.Component { get => this.SpriteComp; set => this.SpriteComp = value; }
        */
        PositionComponentAlpha IHasComponentAlpha<PositionComponentAlpha>.Component { get; set; }
        SpriteComponentAlpha IHasComponentAlpha<SpriteComponentAlpha>.Component { get; set; }
        private PositionComponentAlpha PosComp { get => ((IHasComponentAlpha<PositionComponentAlpha>)this).Component; }
        private SpriteComponentAlpha SpriteComp { get => ((IHasComponentAlpha<SpriteComponentAlpha>)this).Component; }

        private Vector2 Position
        {
            get => PosComp.Position;
            set => PosComp.Position = value;
        }

        private readonly Game game;

        public PlayerAlpha(Game game)
        {
            //*
            //((IHasComponentAlpha<PositionComponentAlpha>)this).Component = new PositionComponentAlpha();
            ((IHasComponentAlpha<PositionComponentAlpha>)this).Component = new PositionComponentAlpha().SetOwner(this);
            //((IHasComponentAlpha<SpriteComponentAlpha>)this).Component = (SpriteComponentAlpha)new SpriteComponentAlpha().SetOwner(this);
            ((IHasComponentAlpha<SpriteComponentAlpha>)this).Component = new SpriteComponentAlpha().SetOwner(this);
            //*/

            //Utils.AddMixins(this);

            //PosComp = new PositionComponentAlpha();
            //SpriteComp = new SpriteComponentAlpha(this);
            //this.GetComponent<PositionComponentAlpha>().SetOwner(this);
            //SpriteComp.SetOwner(this);

            this.game = game;

            this.Position = new Vector2(0, 0);
            this.MovePosition = this.Position;
        }

        public PlayerAlpha(Game game, float x, float y) : this(game)
        {
            this.Position = new Vector2(x, y);
            this.MovePosition = this.Position;
        }

        public void LoadContent(ContentManager content)
        {
            SpriteComp.Sprite = content.Load<Texture2D>("Images/snubbe");
        }

        public void Update(GameTime gameTime)
        {
            var mouseState = Mouse.GetState();

            if (game.IsActive && mouseState.LeftButton == ButtonState.Pressed)
            {
                this.MovePosition = new Vector2(mouseState.X, mouseState.Y);
            }

            this.Position = this.Approach(Position, MovePosition, 2);

        }

        public void Draw(SpriteBatch spriteBatch)
        {
            SpriteComp.Draw(spriteBatch);
        }

        public Vector2 Approach(Vector2 start, Vector2 target, double speed)
        {
            Vector2 error = target - start;
            if (error.LengthSquared() <= Math.Pow(speed,2))
            {
                return target;
            }
            else
            {
                Vector2 dir = Vector2.Normalize(error);
                //return start + new Vector2(dir.X * speed, dir.Y * speed);
                return start + dir * (float) speed;
            }
        }
    }
}