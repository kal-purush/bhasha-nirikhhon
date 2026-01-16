using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AutoClicker.Information;
using AutoClicker.WorkWithDll;

namespace AutoClicker.Execution
{
    public class MouseActions
    {
        private static readonly Dictionary<Clicks, MouseEventFlags[]> ClicksDependancy =
            new()
            {
                { Clicks.Left, new[] { MouseEventFlags.LeftDown, MouseEventFlags.LeftUp } },
                { Clicks.Middle, new[] { MouseEventFlags.MiddleDown, MouseEventFlags.MiddleUp } },
                { Clicks.Right, new[] { MouseEventFlags.RightDown, MouseEventFlags.RightUp } }
            };
        public static void MouseClick(Clicks clickType)
        {
            var clicks = ClicksDependancy[clickType];
            MouseClicks.MouseEvent(clicks[0]);
            MouseClicks.MouseEvent(clicks[1]);
        }
    }
}