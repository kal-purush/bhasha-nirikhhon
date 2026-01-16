// FadeEffect.cs
// James Tyson
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Xna.Framework;

namespace RkoOuttaNowhere.Images.Effects
{
    /// <summary>
    /// FadeEffect produces a fading effect on an image
    /// </summary>
    public class FadeEffect : ImageEffect
    {
        public float FadeSpeed;
        public bool Increase;

        /// <summary>
        /// Default Constructor
        /// </summary>
        public FadeEffect()
        {
            FadeSpeed = 1;
            Increase = false;
        }

        public override void LoadContent(ref Image image)
        {
            base.LoadContent(ref image);
        }

        public override void UnloadContent()
        {
            base.UnloadContent();
        }

        public override void Update(GameTime gameTime)
        {
            base.Update(gameTime);

            // Check if the effect is active
            if (_image.IsActive)
            {
                // Fade in or out based on increase state
                if (!Increase)
                    _image.Alpha -= FadeSpeed * (float)gameTime.ElapsedGameTime.TotalSeconds;
                else
                    _image.Alpha += FadeSpeed * (float)gameTime.ElapsedGameTime.TotalSeconds;

                // Loop through for a flashing effect
                if (_image.Alpha < 0.0f)
                {
                    Increase = true;
                    _image.Alpha = 0.0f;
                }
                else if (_image.Alpha > 1.0f)
                {
                    Increase = false;
                    _image.Alpha = 1.0f;
                }
            }
            // Reset the alpha if the image effect is finished
            else
                _image.Alpha = 1.0f;
        }
    }
}