using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    class Ball
    {
        private Color color;
        private int throwsOfABall;
        private float radius;

        public Ball(Color color,float radius, int throwsOfABall)
        {
            this.color = color;
            this.radius = radius;
            this.throwsOfABall = 0;
        }

        public void Pop()
        {
            radius = 0;
        }

        public void Throw(int throwABall)
        {
            if (radius> 0)
            {
                throwsOfABall++;
            }
            //else Console.WriteLine("Cannot throw a ball if Pop was used(size=0)");
        }


        //Get throws, ball size
        public int GetThrows(int throwsOfABall)
        {
            return throwsOfABall;
        }

        public float GetRadius(float radius)
        {
            return radius;
        }

    }
}