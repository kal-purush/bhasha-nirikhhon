using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AntWars.Board;

namespace AntWars.Helper
{
    class CoordsInView
    {
        private BoardObjects boardObjects;

        public int Width { get; set; }
        public List<Coordinates> circle {get;set;}

        public CoordsInView(int width)
        {
            
        }

        public CoordsInView(int width, BoardObjects boardObjects)
        {
            this.boardObjects = boardObjects;
            Width = width;
            int boxMinX = 0 - Width;
            int boxMinY = 0 - Width;
            int boxMaxX = 0 + Width;
            int boxMaxY = 0 + Width;
            List<Coordinates> coordinatesInsideView = new List<Coordinates>();
            for (int x = boxMinX; x <= boxMaxX; x++)
            {
                for (int y = boxMinY; y <= boxMaxY; y++)
                {
                    double abstand = Math.Pow(0 - x, 2) + Math.Pow(0 - y, 2);
                    if (abstand <= Math.Pow(Width, 2))
                    {
                        coordinatesInsideView.Add(new Coordinates(x, y));
                    }
                }
            }
            circle = coordinatesInsideView;
        }

        public List<Coordinates> getCoordinatesInsideView(Coordinates center)
        {

            List<Coordinates> coordinatesInsideView = new List<Coordinates>();
            foreach (Coordinates c in circle)
            {
                Coordinates toAdd = new Coordinates(c.X + center.X, c.Y + center.Y);
                if(boardObjects.isValidCoords(toAdd))
                {

                coordinatesInsideView.Add(toAdd);
                }
            }
            return coordinatesInsideView;
        }
    }
}