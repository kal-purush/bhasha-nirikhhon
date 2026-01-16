using System.Collections.Generic;
using Microsoft.Xna.Framework;


namespace Introproject
{
    public class AnimatedGameObject : Object2D
    {
        protected Dictionary<string, Animation> animations;

        public AnimatedGameObject(string id = "")
            : base("", 0, id) //<-verander dit indien nodig
        {
            animations = new Dictionary<string, Animation>();
        }
        public void LoadAnimation(string assetname, string id, bool looping, float frameTime = 0.1f)
        {
            Animation animation = new Animation(assetname, looping, frameTime);
            animations[id] = animation;
        }

        public void PlayAnimation(string id)
        {
            if (spriteSheet == animations[id])
                return;
            if (spriteSheet != null)
                animations[id].Mirror = spriteSheet.Mirror; //alles wordt mirror?
            animations[id].play();
            spriteSheet = animations[id];
            origin = new Vector2(spriteSheet.Width / 2, spriteSheet.Height);// boven in het midden

        }

        public override void Update(GameTime gameTime)
        {
            if (spriteSheet == null)
                return;
            Current.Update(gameTime);
            base.Update(gameTime);
        }

        public Animation Current
        {
            get { return spriteSheet as Animation; }
        }


    }
}