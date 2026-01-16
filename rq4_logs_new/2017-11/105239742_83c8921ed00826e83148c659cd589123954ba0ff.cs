using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;

namespace A_Merchants_Tale
{
    class Camera
    {
        float zoom;
        float min;
        float max;
        float zoomChange;
        float offsetX;
        float offsetY;

        Vector2 position;
        float positionChange;

        float rotation;
        float rotationChange;

        Viewport viewport;
        MouseState mouseState;
        Vector2 mousePosition;
        int previousScrollValue;
        KeyboardState keyState;
        Matrix transform;
        Matrix inverseTransform; //So that our mouse input is correct

        public Camera(Viewport view)
        {
            zoom = 1.0f;
            zoomChange = 0.1f;
            min = 0.5f;
            max = 3.0f;
            
            position = Vector2.Zero; //Vector is a struct. Vector2.Zero just initializes all of its components to zero.
            positionChange = 10f;

            rotation = 0.0f;
            rotationChange = 0.1f;

            viewport = view;
            previousScrollValue = 0;
        }

        public float Zoom
        {
            get
            {
                return zoom;
            }
            set
            {
                zoom = value;
            }
        }

        public Matrix Transform
        {
            get
            {
                return transform;
            }
            set
            {
                transform = value;
            }
        }

        //Can be used to get the screen coordinates of objects
        public Matrix InverseTransform
        {
            get
            {
                return inverseTransform;
            }
            set
            {
                inverseTransform = value;
            }
        }

        public Vector2 Position
        {
            get
            {
                return position;
            }
            set
            {
                position = value;
            }
        }

        public float Rotation
        {
            get
            {
                return rotation;
            }
            set
            {
                rotation = value;
            }
        }

        public void Update(MouseState mouse, KeyboardState keys)
        {
            mouseState = mouse;
            keyState = keys;
            Input();
            zoom = Clamp(zoom, min, max); //Restricts zoom to be within 0.1 and 3
            rotation = ClampAngle(rotation); //Makes sure the angle is between pi and -pi

            //This is some Linear Algebra shizz right here
            createMatrices();

            mousePosition = new Vector2(mouse.X, mouse.Y);
            mousePosition = Vector2.Transform(mousePosition, inverseTransform);

            if(zoom != min && zoom != max)
                ZoomOffset(mousePosition);

            createMatrices();

            previousScrollValue = mouseState.ScrollWheelValue;
        }

        public void Input()
        {
            //Check zoom
            if (mouseState.ScrollWheelValue > previousScrollValue)
            {
                zoom += zoomChange;
            }
            else if (mouseState.ScrollWheelValue < previousScrollValue)
            {
                zoom -= zoomChange;
            }

            //Check move
            if (keyState.IsKeyDown(Keys.A))
            {
                position.X -= positionChange;
            }
            if (keyState.IsKeyDown(Keys.D))
            {
                position.X += positionChange;
            }
            if (keyState.IsKeyDown(Keys.W))
            {
                position.Y -= positionChange;
            }
            if (keyState.IsKeyDown(Keys.S))
            {
                position.Y += positionChange;
            }

            //Check rotation
            if (keyState.IsKeyDown(Keys.Left))
            {
                rotation -= rotationChange;
            }
            else if(keyState.IsKeyDown(Keys.Right))
            {
                rotation += rotationChange;
            }
        }

        public float Clamp(float zoom, float min, float max)
        {
            if (zoom < min)
            {
                zoom = min;
            }
            if (zoom > max)
            {
                zoom = max;
            }
            return zoom;
        }

        //Makes sure the angle is between pi and -pi
        public float ClampAngle(float radians)
        {
            while(radians < MathHelper.Pi)
            {
                radians += MathHelper.TwoPi;
            }
            while(radians > MathHelper.Pi)
            {
                radians -= MathHelper.TwoPi;
            }
            return radians;
        }

        //Linear algebra shizz
        public void createMatrices()
        {
            transform = Matrix.CreateRotationZ(rotation) *
                        Matrix.CreateScale(new Vector3(zoom, zoom, 1)) *
                        Matrix.CreateTranslation(position.X, position.Y, 0);
            inverseTransform = Matrix.Invert(transform);
        }

        public void ZoomOffset(Vector2 mouse)
        {
            offsetX = mouse.X * zoomChange;
            offsetY = mouse.Y * zoomChange;

            if (mouseState.ScrollWheelValue > previousScrollValue)
            {
                position.X -= offsetX;
                position.Y -= offsetY;
            }
            else if (mouseState.ScrollWheelValue < previousScrollValue)
            {
                position.X += offsetX;
                position.Y += offsetY;
            }
        }
    }
}