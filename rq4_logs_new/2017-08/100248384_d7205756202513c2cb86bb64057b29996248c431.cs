using System;

using System.Management;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Input;

namespace Game1
{
    public static class AuxInput
    {
        public static Dictionary<Keys, KeyState> KeyPrevState = new Dictionary<Keys, KeyState>();
        
        static AuxInput()
        {
            //遍历枚举
            foreach (Keys k in Enum.GetValues(typeof(Keys)))
            {
                KeyPrevState.Add(k, KeyState.Up);
            }
        }

        public static KeyState GetKeyPrevState(Keys key)
        {
            return KeyPrevState[key];
        }

        public static bool IsKeyPressed(Keys key)
        {
            bool result = false;
            if(GetKeyPrevState(key) == KeyState.Up && Keyboard.GetState().IsKeyDown(key))
            {
                result = true;
            }
            KeyPrevState[key] = (KeyState)Convert.ToInt32(Keyboard.GetState().IsKeyDown(key));
            return result;
            
        }

        public static bool IsKeyReleased(Keys key)
        {
            bool result = false;
            if (GetKeyPrevState(key) == KeyState.Down && Keyboard.GetState().IsKeyUp(key))
            {
                result = true;
            }
            KeyPrevState[key] = (KeyState)Convert.ToInt32(Keyboard.GetState().IsKeyUp(key));//(Keyboard.GetState().IsKeyUp(key))?KeyState.Up:KeyState.Down;
            return result;
        }
    }
}