// Copyright (c) Affectionate Dove <contact@affectionatedove.com>.
// Licensed under the Affectionate Dove Limited Code Viewing License.
// See the LICENSE file in the repository root for full license text.

using HenHen.Framework.Numerics;
using System.Numerics;

namespace HenHen.Framework.Graphics2d.Worlds
{
    public class Camera2D
    {
        /// <summary>
        ///     The point the camera is looking at.
        /// </summary>
        public Vector2 Target { get; set; }

        /// <summary>
        ///     The amount of visible vertical space.
        /// </summary>
        public float FovY { get; set; }

        /// <summary>
        ///     Returns a rectangle that defines the area
        ///     of the world that is visible.
        /// </summary>
        /// <remarks>
        ///     If either coordinate of <paramref name="renderingSpaceSize"/>
        ///     is less than or equal to 0, returns default rectangle.
        /// </remarks>
        public RectangleF GetVisibleArea(Vector2 renderingSpaceSize)
        {
            var size = GetVisibleAreaSize(renderingSpaceSize);
            if (size == Vector2.Zero)
                return new();
            var halfHeight = size.Y * 0.5f;
            var halfWidth = size.X * 0.5f;
            return new(Target.X - halfWidth, Target.X + halfWidth, Target.Y - halfHeight, Target.Y + halfHeight);
        }

        public Vector2 GetVisibleAreaSize(Vector2 renderingSpaceSize)
        {
            if (renderingSpaceSize.X is <= 0 || renderingSpaceSize.Y is <= 0)
                return new();
            var height = FovY;
            var widthToHeightRatio = renderingSpaceSize.X / renderingSpaceSize.Y;
            var width = widthToHeightRatio * height;
            return new(width, height);
        }
    }
}