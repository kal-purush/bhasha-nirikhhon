using System;

namespace door
{
    public class Door
    {
        private static string estadoPuerta = "Open";

        private const int key = 12345;



        public string Close()
        {
            if (estadoPuerta == "Close")
            {

                return Console.Error.ToString();
            }
            else
            {
                estadoPuerta = "Close";
                return estadoPuerta;
            }
        }

        public string Open()
        {
            if (estadoPuerta == "Open")
            {

                return Console.Error.ToString();
            }
            else
            {
                estadoPuerta = "Open";
                return estadoPuerta;
            }
        }

        public string unLock(int keyUser)
        {
            if (estadoPuerta == "unLock" || key != keyUser)
            {
                return Console.Error.ToString();
            }
            else
            {
                estadoPuerta = "unLock";
                return estadoPuerta;
            }
        }

        public string Lock()
        {
            if (estadoPuerta == "Lock")
            {

                return Console.Error.ToString();
            }
            else
            {
                estadoPuerta = "Lock";
                return estadoPuerta;
            }
        }

        public bool isOpen()
        {
            if (estadoPuerta == "Open")
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool isClose()
        {
            if (estadoPuerta == "Close")
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool isLock()
        {
            if (estadoPuerta == "Lock")
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public bool isUnLock()
        {
            if (estadoPuerta == "unLock")
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}