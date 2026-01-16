using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Input;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Assignment3
{
    class Camera : GameComponent
    {
        //Attributes (variables)
        private Vector3 cameraPos;//camera postion (x,y,z)
        private Vector3 cameraRot;//rotation
        private float cameraSpeed;//movement speed
        private Vector3 LookAt;//What the camera is looking at
        private Vector3 mouseRotationBuffer;
        private MouseState curMS;
        private MouseState prevMS;


        //properties

        public Vector3 Position
        {
            get { return cameraPos; }
            set
            {
                cameraPos = value;
                UpdateLookAt();
            }
        }

        public Vector3 Rotation
        {
            get { return cameraRot; }
            set
            {
                cameraRot = value;
                UpdateLookAt();
            }
        }


        public Matrix Projection
        {
            get;
            protected set;
        }

        public Matrix View
        {
            get
            {
                return Matrix.CreateLookAt(cameraPos, LookAt, Vector3.Up);
            }
        }


        /// <summary>
        /// Constructor.
        /// sets up the camera.
        /// </summary>
        /// <param name="game">game component</param>
        /// <param name="position">camera's position to set</param>
        /// <param name="rotation">camera's rotation to set</param>
        /// <param name="speed">camera's speed</param>
        public Camera(Game game, Vector3 position, Vector3 rotation, float speed) : base(game)
        {
            cameraSpeed = speed;
            Projection = Matrix.CreatePerspectiveFieldOfView(MathHelper.ToRadians(45f), Game.GraphicsDevice.Viewport.AspectRatio, 0.05f, 1000f);

            //set cam pos and rot
            MoveTo(position, rotation);

            prevMS = Mouse.GetState();
        }

        //set camera Pos and Rot
        private void MoveTo(Vector3 Pos, Vector3 Rot)
        {
            Position = Pos;
            Rotation = Rot;
        }

        //update look at
        private void UpdateLookAt()
        {
            //build rot Matrix
            Matrix rotationMatrix = Matrix.CreateRotationX(cameraRot.X) * Matrix.CreateRotationY(cameraRot.Y);

            //create lookat offset (change in lookAt)
            Vector3 LookAtOffset = Vector3.Transform(Vector3.UnitZ, rotationMatrix);

            //Update camera's lookAt vector
            LookAt = cameraPos + LookAtOffset;
        }

        //Move camera
        public void Move(Vector3 amount)
        {
            Matrix rotate = Matrix.CreateRotationY(cameraRot.Y);
            Vector3 movement = new Vector3(amount.X, amount.Y, amount.Z);
            movement = Vector3.Transform(movement, rotate);

            MoveTo((cameraPos + movement), Rotation);
        }
        
        //update method
        public override void Update(GameTime gameTime)
        {
            float DeltaTime = (float)gameTime.ElapsedGameTime.TotalSeconds;

            curMS = Mouse.GetState();

            KeyboardState kbs = Keyboard.GetState();

            Vector3 moveVector = Vector3.Zero; //which direction are we going?

            if(kbs.IsKeyDown(Keys.W))
            {
                moveVector.Z = 1;
            }

            if(kbs.IsKeyDown(Keys.S))
            {
                moveVector.Z = -1;
            }

            if(kbs.IsKeyDown(Keys.A))
            {
                moveVector.X = 1;
            }

            if(kbs.IsKeyDown(Keys.D))
            {
                moveVector.X = -1;
            }

            //if we are moving
            if(moveVector != Vector3.Zero)
            {
                moveVector.Normalize();
                moveVector *= DeltaTime * cameraSpeed;

                //move camera
                Move(moveVector);
            }

            //Mouse Movement
            float deltaX;
            float deltaY;

            if(curMS != prevMS)
            {
                //cache mouse location (to always be relative to middle of screen)
                deltaX = curMS.X - (Game.GraphicsDevice.Viewport.Width / 2);
                deltaY = curMS.Y - (Game.GraphicsDevice.Viewport.Height / 2);

                //smooths mouse movement; creates rotation
                mouseRotationBuffer.X -= 0.01f * deltaX * DeltaTime;
                mouseRotationBuffer.Y -= 0.01f * deltaY * DeltaTime;

                if(mouseRotationBuffer.Y < MathHelper.ToRadians(-75f))
                {
                    mouseRotationBuffer.Y = mouseRotationBuffer.Y - (mouseRotationBuffer.Y - MathHelper.ToRadians(-75f));
                }

                if(mouseRotationBuffer.Y > MathHelper.ToRadians(75f))
                {
                    mouseRotationBuffer.Y = mouseRotationBuffer.Y - (mouseRotationBuffer.Y - MathHelper.ToRadians(75f));
                }

                //limit Y to only 75 for looking up or down; wrap X (make it 360)
                Rotation = new Vector3(-MathHelper.Clamp(mouseRotationBuffer.Y, MathHelper.ToRadians(-75f), MathHelper.ToRadians(75f)), MathHelper.WrapAngle(mouseRotationBuffer.X), 0);

                deltaX = 0;
                deltaY = 0;

            }

            //set cursor back to the middle of screen
            Mouse.SetPosition(Game.GraphicsDevice.Viewport.Width / 2, Game.GraphicsDevice.Viewport.Height / 2);

            prevMS = curMS;

            base.Update(gameTime);
        }

        
    }
}