using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xna.Framework;

namespace myGame
{
    class PowerUpManager : GameObject
    {
        private static System.Random rnd = new System.Random((int)DateTime.Now.Ticks);
        private int status;
        private bool isActive;
        private string gunName;

        public PowerUpManager(KittenWarsGame game)
            : base(game)
        {
            isActive = false;
            gunName = "GUN";
        }

        public override string Name { get { return "MANAGER"; } }

        public override void Initialize()
        {
           // no initialization needed
        }

        public override void LoadContent()
        {
            // no content needed
        }


        public void Dispense()
        {
            status = rnd.Next(0, 2);

            switch (status)
            {
                case 0:
                    game.AddObject(new PowerUpPierce(game));
                    break;

                case 1:
                    game.AddObject(new PowerUpSpread(game));
                    break;

                default:
                    Console.WriteLine("Not a valid power up value");
                    break;
            }
        }


        public void ToggleActive()
        {
            isActive = true;
        }

        public override void Draw(GameTime gameTime)
        {
            // nothing to draw
        }

        public override void Update(GameTime gameTime)
        {
            if(isActive)

            {
                var gun = (Gun)game.GameObjectByName(gunName);
                //game.RemoveObject(gun);
                switch (status)
                {
                    case 0:
                        //game.setGun(new PierceGun(game));
                        break;

                    case 1:
                        //game.setGun(new SpreadGun(game));
                        break;

                    default:
                        // should not occur, but if it does remake the original gun
                        //game.setGun(new Gun(game));
                        break;
                }
            }

            isActive = false;
        }
    }
}