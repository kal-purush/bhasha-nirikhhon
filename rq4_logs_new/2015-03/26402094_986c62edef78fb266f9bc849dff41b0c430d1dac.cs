using OpenTK;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Gal3DEngine.UTILS
{
    public class Box
    {
        public float radiusX;
        public float radiusY;
        public float radiusZ;

        public Vector3 origin;

        public Box(float radiusX, float radiusY, float radiusZ, Vector3 origin)
        {
            this.radiusX = radiusX;
            this.radiusY = radiusY;
            this.radiusZ = radiusZ;
            this.origin = origin;
        }

        private Vector3 GetMin()
        {
            return new Vector3(origin.X - radiusX, origin.Y - radiusY, origin.Z - radiusZ);
        }

        private Vector3 GetMax()
        {
            return new Vector3(origin.X + radiusX, origin.Y + radiusY, origin.Z + radiusZ);
        }

        public static bool IsColliding(Box a, Box b)
        {
            bool collide = (a.GetMax().X >= b.GetMin().X && a.GetMin().X <= b.GetMax().X)
            && (a.GetMax().Y >= b.GetMin().Y && a.GetMin().Y <= b.GetMax().Y)
            && (a.GetMax().Z >= b.GetMin().Z && a.GetMin().Z <= b.GetMax().Z);
            if (collide)
                return collide;
            else return collide;
        }

        public static bool IsPointContained(Box a, Vector3 point)
        {
            var min = a.GetMin();
            var max = a.GetMax();
            return  point.X >= min.X && point.X <= max.X &&
                    point.Y >= min.Y && point.Y <= max.Y &&
                    point.Z >= min.Z && point.Z <= max.Z;
        }

        public void DrawCube(Screen screen, Matrix4 world, Matrix4 view, Matrix4 projection)
        {
            DrawCube(screen, world, view, projection, new Color3(0, 255, 0));
        }

        public void DrawCube(Screen screen, Matrix4 world, Matrix4 view, Matrix4 projection, Color3 color)
        {
            var min = GetMin();
            var max = GetMax();

            Vector3[] wireFrames = new Vector3[8];
            wireFrames[0] = new Vector3(min.X, min.Y, min.Z); // origin
            wireFrames[1] = new Vector3(max.X, min.Y, min.Z);
            wireFrames[2] = new Vector3(min.X, max.Y, min.Z);
            wireFrames[3] = new Vector3(min.X, min.Y, max.Z);
            wireFrames[4] = new Vector3(max.X, max.Y, min.Z);
            wireFrames[5] = new Vector3(max.X, min.Y, max.Z);
            wireFrames[6] = new Vector3(min.X, max.Y, max.Z);
            wireFrames[7] = new Vector3(max.X, max.Y, max.Z);

            for (int i = 0; i < wireFrames.Length; i++)
            {
                Vector4 tmp = new Vector4(wireFrames[i]);
                tmp.W = 1;
                ShaderHelper.TransformPosition(ref tmp, world * view * projection, screen);
                wireFrames[i] = new Vector3(tmp);
            }
            /*
                 .+------+ 
                 /|     /| 
                +-+----+ | 
                | |    | | 
                | +----+-+ 
                |/     |/
                +------+ 
             */
            //1-3
            screen.DrawLine(wireFrames[0], wireFrames[1], color);
            screen.DrawLine(wireFrames[0], wireFrames[2], color);
            screen.DrawLine(wireFrames[0], wireFrames[3], color);
            //4
            screen.DrawLine(wireFrames[1], wireFrames[4], color);
            screen.DrawLine(wireFrames[2], wireFrames[4], color);
            screen.DrawLine(wireFrames[7], wireFrames[4], color);
            //5
            screen.DrawLine(wireFrames[1], wireFrames[5], color);
            screen.DrawLine(wireFrames[3], wireFrames[5], color);
            screen.DrawLine(wireFrames[7], wireFrames[5], color);
            //6
            screen.DrawLine(wireFrames[2], wireFrames[6], color);
            screen.DrawLine(wireFrames[3], wireFrames[6], color);
            screen.DrawLine(wireFrames[7], wireFrames[6], color);
        }

    }
}