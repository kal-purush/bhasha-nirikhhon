using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.GamerServices;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Media;

namespace Aerial_Mayhem
{
    enum States
    {
        Up, Down, Right, Left, Death, Hurt, Idle
    }
    class AnimatedSprite
    {
        // Attributes
        ArrayListExtended images;
        ArrayListExtended frameCounts;

        States st;

        Rectangle pos;
        Rectangle frame;

        int verticalFrames;
        int horizontalFrames;
        int currentFrame;

        float timePerFrame;
        float timer;


        // Methods

        public AnimatedSprite(Rectangle position, float timeperFrame)
        {
            //gets how many states are in enum
            //and add them so they can be replaced later
            st = States.Idle;
            images = new ArrayListExtended();
            frameCounts = new ArrayListExtended();
            pos = position;
            foreach (States s in Enum.GetValues(typeof(States)))
            {
                images.Add(null);
                frameCounts.Add(null);
            }
            currentFrame = 0; this.timePerFrame = timeperFrame; timer = 0.0f;
        }
        //load animation file for state
        public void LoadContent(ContentManager Content, States state, string filepath, int frameCount, int verticalFrames, int horizontalFrames)
        {
            Texture2D texture = Content.Load<Texture2D>(filepath);
            images[(int)state] = texture;
            frameCounts[(int)state] = frameCount;
            this.verticalFrames = verticalFrames;
            this.horizontalFrames = horizontalFrames;
            frame = new Rectangle(0, 0, texture.Width / horizontalFrames, texture.Height / verticalFrames);
        }

        public void Update(GameTime gameTime)
        {
            timer += (float)gameTime.ElapsedGameTime.TotalSeconds;

            // Update my animation
            // Update my currentFrame pointer
            if (timer >= timePerFrame)
            {
                currentFrame = (currentFrame + 1) % (int)frameCounts[(int)st];
                timer = timer - timePerFrame;
            }
        }

        public void Draw(SpriteBatch spriteBatch)
        {

            frame.Y = frame.Height * (currentFrame / horizontalFrames % verticalFrames);
            frame.X = (currentFrame % horizontalFrames) * frame.Width;


            //TODO make the animation
            spriteBatch.Draw((Texture2D)images[(int)st], pos, frame, Color.White);

        }


    }
}