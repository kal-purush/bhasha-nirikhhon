using Microsoft.Xna.Framework.Graphics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SeriousGame
{
    class TextureManager
    {
        private static TextureManager _instance;
        public static TextureManager Instance
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new TextureManager();
                }
                return _instance;
            }
        }


        public Texture2D Vlieg;


        public void Load()
        {
            this.Vlieg = ScreenManager.Instance.Content.Load<Texture2D>("vlieg");
        }
    }
}