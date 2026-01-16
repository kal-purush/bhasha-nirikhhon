using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Drawing;

namespace VisualProgrammingProject
{
    class CloudDocs
    {
        private int width;
        private int height;
        private int[] heights;
        private int[] takenHeights;
        private int lenght;
        private List<Clouds> clouds;
        public CloudDocs(int monitorWidth, int monitorHeight)
        {
            int distance = 100;
            lenght = monitorHeight/100;
            heights = new int[lenght];
            takenHeights =  new int [lenght];
            for (int i = 0; i < lenght; i++)
            {
                takenHeights[i] = 0;
                heights[i] = distance;
                distance += 100;
            }
            clouds = new List<Clouds>();
            this.width = monitorWidth;
            this.height = monitorHeight;
        }
        public void move()
        {
            for (int i = 0; i < clouds.Count; i++)
            {
                bool t = clouds.ElementAt(i).move();
                if (t)
                {
                    clouds.RemoveAt(i);
                }
            }
        }
        public bool checkIfCollide(Point playerLocation, int playerHeight, int playerWidth, out bool isAlive, ref int score)
        {
            Point left = new Point(playerLocation.X , playerLocation.Y + playerHeight);
            Point right = new Point(playerLocation.X + playerWidth , playerLocation.Y + playerHeight);
            isAlive = true;
            for (int i = 0; i < clouds.Count; i++)
            {
                
                bool t = clouds.ElementAt(i).checkPlayerPosition(left, right, playerHeight, playerWidth, out isAlive, ref score);
                if (t)
                {
                    return true;
                }

            }
            return false;
        }
        public int makeHeight(int number)
        {
            if (takenHeights[number] < 2)
            {
                takenHeights[number]++;
                return number;
            }
            for (int i = 0; i < lenght; i++)
            {
                if (takenHeights[i] < 1)
                {
                    takenHeights[i]++;
                    return i;
                }
            }
            for (int i = 0; i < lenght; i++)
            {
                takenHeights[i] = 0;
            }
            return number;
        }
        public void addRectangle()
        {
            Random rnd = new Random();
            int y = heights[makeHeight(rnd.Next(0, lenght))];
            int x = width;
            Clouds rect = new Clouds(x, y);
            clouds.Add(rect);
        }
        public void draw(Graphics g)
        {
            foreach (Clouds t in clouds)
            {
                t.draw(g);
            }
        }
    }
}