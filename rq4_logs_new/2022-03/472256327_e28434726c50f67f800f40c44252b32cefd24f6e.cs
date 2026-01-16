using ClassLibrary1.Coords;

namespace ClassLibrary1.Arms
{
    public class Arm : IMovable
    {
        public Coordinate location { get; private set; }
        public string name { get; init; }
        public bool isOccupied { get; private set; }
        public string currentAction { get; private set; }
        public Arm(string name, Coordinate c)
        {
            this.name = name;
            location = new Coordinate(c);
            isOccupied=false;
            currentAction = "idle";
        }
        public void MoveX(int offset)
        {
            location.x += offset;
            Thread.Sleep(Math.Abs(offset) * 10);
        }
        public void MoveY(int offset)
        {
            location.y += offset;
            Thread.Sleep(Math.Abs(offset) * 10);
        }
        public void MoveZ(int offset)
        {
            location.z += offset;
            Thread.Sleep(Math.Abs(offset) * 10);
        }
        public void Zero()
        {
            location.SetZero();
        }
        public string toString()
        {
            return $"{name}\tCoordinations:({location.x},{location.y},{location.z})";
        }

        internal void ShiftX(int offset)
        {
            location.x+=offset;
        }
        internal void ShiftY(int offset)
        {
            location.y += offset;
        }
        internal void ShiftZ(int offset)
        {
            location.z += offset;
        }

        public bool Move(params int[] offset)
        {
            int len = offset.Length;
            int x = len > 0 ? offset[0] : 0;
            int y = len > 1 ? offset[1] : 0;
            int z = len > 2 ? offset[2] : 0;
            isOccupied = true;
            currentAction = "Move";
            //Calling the seperate Move_ methods in case that there are any checks to be made
            MoveX(x);
            MoveY(y);
            MoveZ(z);
            isOccupied = false;
            currentAction = "idle";
            return true;
        }

        public bool PerformShortAction()
        {
            isOccupied = true;
            currentAction = "Short action";
            Thread.Sleep(2000);
            isOccupied = false;
            currentAction = "idle";
            return true;
        }

        public bool PerformMediumAction()
        {
            currentAction = "Medium action";
            isOccupied = true;
            Thread.Sleep(5000);
            isOccupied = false;
            currentAction = "idle";
            return true;
        }

        public bool PerformLongAction()
        {
            currentAction = "Long action";
            isOccupied = true;
            Thread.Sleep(10000);
            isOccupied = false;
            currentAction = "idle";
            return true;
        }
    }
}