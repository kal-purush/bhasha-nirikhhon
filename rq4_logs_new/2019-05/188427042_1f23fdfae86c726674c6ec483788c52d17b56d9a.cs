using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HeadSoccer.Classes
{
    class Player
    {
        public static int x, y, speed;
        public static string direction;
        int jumpHeight = 40, counter = 1;
        bool flag = true;

        public Player(int _x, int _y, int _speed, string _direction)
        {
            x = _x;
            y = _y;
            speed = _speed;
            direction = _direction;
        }

        public void PlayerMove(string _direction)
        {
            if (_direction == "left")
            {
                x = x - speed;
            }

            if (_direction == "right")
            {
                y = y - speed;
            }
        }

        public void PlayerJump()
        {
            if (flag == true)
            {
                flag = false;

                if (counter == 1)
                {
                    y = y + 15;
                }

                if (counter == 2)
                {
                    y = y + 25;
                }

                if (y >= 710)
                {
                    flag = true;
                }
            }

        }

    }
}