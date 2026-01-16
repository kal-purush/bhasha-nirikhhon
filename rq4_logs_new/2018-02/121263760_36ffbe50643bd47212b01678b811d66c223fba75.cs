using System;
using System.Collections.Generic;
using Microsoft.Xna.Framework;
using Microsoft.Xna.Framework.Input;

namespace OneRoomRPGJam
{
	public static class Input
	{
		static KeyboardState keyboardState;
		static MouseState mouseState;

		public static void Update(GameTime gameTime)
		{
			keyboardState = Keyboard.GetState();
			mouseState = Mouse.GetState();

			foreach (Keys k in GetPressedKeys())
			{
				Console.WriteLine("Key: " + k + " pressed"); 
			}
		}

		public static KeyboardState GetKeyboardState()
		{
			return keyboardState; 
		}

		public static MouseState GetMouseState()
		{
			return mouseState; 
		}

		public static Keys[] GetPressedKeys()
		{
			return keyboardState.GetPressedKeys();
		}
	}
}