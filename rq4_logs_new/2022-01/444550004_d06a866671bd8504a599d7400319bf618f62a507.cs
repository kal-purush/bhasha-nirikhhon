using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;

namespace nekoT
{
    public class SpriteAtlas : ICloneable
    {
        public Texture2D Texture { get; set; }
        public SpriteAtlas parent;
        public Vector2 Direction;
        public Vector2 Position;
        public Vector2 Origin { get; private set; }
        public bool isRemoved = false;
        public float Rotation { get; set; }
        public int CurrentFrame { set; get; }
        //public int TotalFrames { get; set; }
        public int Width,Height = 1;
        public SpriteEffects Effect;
        public SpriteAtlas(Texture2D spritesheet, int rows, int columns, int frame)
        {
            Texture = spritesheet;
            int width = spritesheet.Width / columns;
            int height = spritesheet.Height / rows;
            //_columns = columns;
            CurrentFrame = frame;
            Origin = new(width * 0.5f, height * 0.5f);
        }
        public virtual void Update(GameTime gameTime, Camera camera) { }
        public object Clone()
        {
            return MemberwiseClone();
        }
        /*private Texture2D Slice(Texture2D org, int x, int y) // x is position on sprite sheet, same as y
        {
            Texture2D tex = new(org.GraphicsDevice, width, height);
            var data = new Color[width * height];
            org.GetData(0, new(width * (CurrentFrame % columns), height * (CurrentFrame / columns), width, height), data, 0, data.Length);
            tex.SetData(data);
            return tex;
        }*/ // currently implemented in class, UPD: Hey i removed it due many devs says Slice is messy asf so will use 
            //what they recommend ;)
        public void Draw(SpriteBatch spritebatch)
        {
            spritebatch.Draw(Texture,
            new((int)Position.X, (int)Position.Y, Width, Height),
            new(width * (CurrentFrame % columns), height * (CurrentFrame / columns), width, height),
            Color.White, Rotation, Origin, Effect, 1);
        }
    }
}