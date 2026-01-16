using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SFML.Graphics;
using SFML.Window;

namespace ProjectSheep{
    class Tile{
        RectangleShape shape;
        public bool Walkable { get; private set; }
        public Tile(Color color, Vector2f position, bool walkable, Vector2f size){
            shape = new RectangleShape(size);
            shape.FillColor = color;
            shape.Position = position;
            Walkable = walkable;
        }

        public void Draw(RenderWindow win){
            win.Draw(shape);
        }
    }
}