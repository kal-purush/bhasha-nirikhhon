using BetterGameEngine.Input;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace BetterGameEngine.Input
{
    public class InputMouse : InputKey
    {
        public enum mouseType
        {
            lmb,
            rmb,
            scr
        }
        public mouseType mouseButtonType;

        public InputMouse(type _type, mouseType _mType) : base(_type)
        {
            this.assignedType = _type;
            this.mouseButtonType = _mType;
        }
    }
}