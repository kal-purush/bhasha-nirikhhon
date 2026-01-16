using ClassLibrary1.Coords;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClassLibrary1.Arms;
using System.Collections;

namespace ClassLibrary1.Robot
{
    public class Robot : IMovable
    {
        public Coordinate location { get; private set; }
        public Dictionary<string, Arm> arms { get; private set; } //Faster than hashtable
        public Robot(int num, params string[] names)
        {
            arms = new Dictionary<string, Arm>();
            location = new Coordinate();
            AddArms(num, names);
        }

        private void AddArms(int num, string[] names)
        {
            foreach (string name in names)
            {
                arms.Add(name, new Arm(name));
            }
        }
        public void MoveX(int offset)
        {
            location.x += offset;
        }
        public void MoveY(int offset)
        {
            location.y += offset;
        }
        public void MoveZ(int offset)
        {
            location.z += offset;
        }
        public void Zero()
        {
            location.SetZero();
        }
        public string toString()
        {
            string str =  @$"Robot coords: ({location.x},{location.y},{location.z}){Environment.NewLine}Arms: {arms.Count}{Environment.NewLine}";
            foreach(var (key, value) in arms)
            {
                str += $"{value.toString()}{Environment.NewLine}";
            }
            return str;
        }
        public void MoveArm(string armName, int x, int y, int z)
        {
            Arm usedArm = null;
            if(arms.TryGetValue(armName, out usedArm))
            {
                usedArm.MoveArm(x, y, z);
            }
            else
            {
                //Throw exception
            }
        }
    }
}