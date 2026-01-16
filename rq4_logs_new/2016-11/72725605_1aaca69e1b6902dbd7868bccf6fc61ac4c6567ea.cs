using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using SFML;
using SFML.System;
using SFML.Window;
using SFML.Graphics;
using SFML.Audio;

namespace SFML_Prog2_Gruppe1
{
    public class EnemyNPC : Character
    {

        public EnemyNPC() : base()
        {
            characterType = CharacterID.EnemyNPC;
            health = 25;
            stamina = 25;
            damage = 10;
            armor = 10;

            characterTexture = new Texture("Character/EnemyNPC.png");
            characterSprite.Texture = characterTexture;
            characterSprite.Position = new Vector2f(350, 350);
        }

        public override void Update(Tile[,] room)
        {
            base.Update(room);
        }

    }
}