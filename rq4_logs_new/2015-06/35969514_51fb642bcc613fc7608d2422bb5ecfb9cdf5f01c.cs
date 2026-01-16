using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace GGFanGame.Game.Level
{
    class StageCamera
    {
        private double _scale = 2d;
        private Vector2 _offset = Vector2.Zero;

        /// <summary>
        /// The zoom of the camera.
        /// </summary>
        /// <returns></returns>
        public double scale
        {
            get { return _scale; }
            set { _scale = value; }
        }

        /// <summary>
        /// The offset of the camera.
        /// </summary>
        /// <returns></returns>
        public Vector2 offset
        {
            get { return _offset; }
            set { _offset = value; }
        }
    }
}