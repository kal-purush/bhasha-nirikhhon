using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SFML.Graphics;

namespace SFML_Prog2_Gruppe1
{
    abstract class Character
    {
        private enum ID
        {
            Player = 1,
            EnemyNPC = 2,
            QuestNPC = 3
        }

        private Sprite character;
        private Texture texture;
        private Vector2f velocity = new Vector2f();


        public Character()
        {
            character = new Sprite();

            switch (ID)
            {
                // Load texture based on ID
                case 1:
                    texture = new Texture("");
                    break;
                case 2:
                    texture = new Texture("");
                    break;
                case 3:
                    texture = new Texture("");
                    break;
            }

        }

        // Get and Setter for movement
        public void SetVelocity(float x, float y)
        {
            velocity.X = x;
            velocity.Y = y;
        }

        public Vector2f GetVelocity()
        {
            this.velocity = velocity;
            return velocity;
        }

        public abstract void Draw();
        public abstract void Update();

        // TODO: Method to handle input?
    }
}