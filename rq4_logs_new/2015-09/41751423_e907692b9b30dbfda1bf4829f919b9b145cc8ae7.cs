using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Audio;
using Microsoft.Xna.Framework.Content;
using Microsoft.Xna.Framework.GamerServices;
using Microsoft.Xna.Framework.Graphics;
using Microsoft.Xna.Framework.Input;
using Microsoft.Xna.Framework.Media;

namespace mst_boredom_remover
{
    public class Menu : UIObject
    {
        private Texture2D texture;
        private Vector2 position;
        private List<UIObject> controls;
        private Color color;
        private int thisId;

        private bool active = false;

        public Menu(Texture2D texture, Vector2 position, List<UIObject> controls, Color color, int thisId)
            : base(texture, position)
        {
            this.texture = texture;
            this.position = position;
            this.controls = controls;
            this.color = color;
            this.thisId = thisId;
        }

        public void activate()
        {
            active = true;
        }

        public void deactivate()
        {
            active = false;
            // reset?
        }

        public override void changeContext(int id)
        {
            if (id != thisId)
            {
                deactivate();
            }
            else if (id == thisId)
            {
                activate();
            }
        }

        public override void Update(GameTime gt)
        {
            if (active)
            {
                foreach (UIObject x in controls)
                {
                    x.Update(gt);
                }
                //base.Update(gt);
            }
        }

        public override void Draw(SpriteBatch sb)
        {
            if (active)
            {
                sb.Draw(texture, position, color);
                foreach (UIObject x in controls)
                {
                    x.Draw(sb);
                }
                //base.Draw(sb);
            }
        }
    }
}