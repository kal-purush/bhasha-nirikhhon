using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Graphics;

namespace RkoOuttaNowhere.Ui
{
    public class LevelSelectGui : Gui
    {
        private List<Button> _nodes;
        private Action _nodeHandler;

        public LevelSelectGui()
            : base()
        {
            _nodes = new List<Button>();
        }

        public override void LoadContent()
        {
            base.LoadContent();
        }

        public override void UnloadContent()
        {
            base.UnloadContent();
        }

        public override void Update(GameTime gameTime)
        {
            base.Update(gameTime);

            foreach (Button b in _nodes)
                b.Update(gameTime);
        }

        public override void Draw(SpriteBatch spriteBatch)
        {
            base.Draw(spriteBatch);

            foreach (Button b in _nodes)
                b.Draw(spriteBatch);
        }

        public override void LoadNodes(List<Point> points, Action handler, string path)
        {
            _nodeHandler = handler;

            foreach (Point p in points)
            {
                Button b = new Button();
                b.LoadContent(path, new Vector2(p.X, p.Y), HandleNodeClick);
                _nodes.Add(b);
            }
        }

        public void HandleNodeClick(object sender, EventArgs e)
        {

        }


    }
}